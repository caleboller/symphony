defmodule SymphonyElixir.ClaudeCode.Runner do
  @moduledoc """
  Claude Code CLI runner implementation.
  
  Runs Claude Code via CLI in --print --verbose --output-format stream-json mode.
  Provides the same interface as AppServer for seamless integration.
  """

  require Logger
  alias SymphonyElixir.Config

  @port_line_bytes 1_048_576
  @max_stream_log_bytes 1_000

  @type session :: %{
          port: port(),
          metadata: map(),
          workspace: Path.t()
        }

  @spec run(Path.t(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(workspace, prompt, issue, opts \\ []) do
    with {:ok, session} <- start_session(workspace) do
      try do
        run_turn(session, prompt, issue, opts)
      after
        stop_session(session)
      end
    end
  end

  @spec start_session(Path.t()) :: {:ok, session()} | {:error, term()}
  def start_session(workspace) do
    with :ok <- validate_workspace_cwd(workspace),
         {:ok, port} <- start_port(workspace) do
      metadata = port_metadata(port)
      expanded_workspace = Path.expand(workspace)

      {:ok,
       %{
         port: port,
         metadata: metadata,
         workspace: expanded_workspace
       }}
    end
  end

  @spec run_turn(session(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_turn(%{port: port, metadata: metadata, workspace: workspace}, prompt, issue, opts \\ []) do
    on_message = Keyword.get(opts, :on_message, &default_on_message/1)

    Logger.info("Claude Code session started for #{issue_context(issue)}")

    # Send the prompt to Claude Code
    send_prompt_to_port(port, prompt)

    emit_message(
      on_message,
      :session_started,
      %{
        session_id: generate_session_id(),
        thread_id: generate_thread_id(),
        turn_id: generate_turn_id()
      },
      metadata
    )

    case await_turn_completion(port, on_message, workspace) do
      {:ok, result} ->
        Logger.info("Claude Code session completed for #{issue_context(issue)}")

        {:ok,
         %{
           result: result,
           session_id: generate_session_id(),
           thread_id: generate_thread_id(),
           turn_id: generate_turn_id()
         }}

      {:error, reason} ->
        Logger.warning("Claude Code session ended with error for #{issue_context(issue)}: #{inspect(reason)}")

        emit_message(
          on_message,
          :turn_ended_with_error,
          %{
            session_id: generate_session_id(),
            reason: reason
          },
          metadata
        )

        {:error, reason}
    end
  end

  @spec stop_session(session()) :: :ok
  def stop_session(%{port: port}) when is_port(port) do
    stop_port(port)
  end

  defp validate_workspace_cwd(workspace) when is_binary(workspace) do
    workspace_path = Path.expand(workspace)
    workspace_root = Path.expand(Config.workspace_root())

    root_prefix = workspace_root <> "/"

    cond do
      workspace_path == workspace_root ->
        {:error, {:invalid_workspace_cwd, :workspace_root, workspace_path}}

      not String.starts_with?(workspace_path <> "/", root_prefix) ->
        {:error, {:invalid_workspace_cwd, :outside_workspace_root, workspace_path, workspace_root}}

      true ->
        :ok
    end
  end

  defp start_port(workspace) do
    executable = System.find_executable("claude")

    if is_nil(executable) do
      {:error, :claude_not_found}
    else
      port =
        Port.open(
          {:spawn_executable, String.to_charlist(executable)},
          [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            args: command_args(),
            cd: String.to_charlist(workspace),
            line: @port_line_bytes
          ]
        )

      {:ok, port}
    end
  end


  defp command_args do
    model = Config.claude_code_model()
    max_tokens = Config.claude_code_max_tokens()

    base_args = [
      ~c"--print",
      ~c"--verbose",
      ~c"--output-format",
      ~c"stream-json",
      ~c"--dangerously-skip-permissions"
    ]

    model_args = if model, do: [~c"--model", String.to_charlist(model)], else: []
    token_args = if max_tokens, do: [~c"--max-tokens", String.to_charlist(to_string(max_tokens))], else: []

    # Note: prompt will be sent via stdin, not as CLI arg
    base_args ++ model_args ++ token_args
  end

  defp send_prompt_to_port(port, prompt) do
    Port.command(port, prompt <> "\n")
  end

  defp port_metadata(port) when is_port(port) do
    case :erlang.port_info(port, :os_pid) do
      {:os_pid, os_pid} ->
        %{claude_code_pid: to_string(os_pid)}

      _ ->
        %{}
    end
  end

  defp await_turn_completion(port, on_message, workspace) do
    receive_loop(port, on_message, Config.claude_code_turn_timeout_ms(), "", workspace)
  end

  defp receive_loop(port, on_message, timeout_ms, pending_line, workspace) do
    receive do
      {^port, {:data, {:eol, chunk}}} ->
        complete_line = pending_line <> to_string(chunk)
        handle_incoming(port, on_message, complete_line, timeout_ms, workspace)

      {^port, {:data, {:noeol, chunk}}} ->
        receive_loop(
          port,
          on_message,
          timeout_ms,
          pending_line <> to_string(chunk),
          workspace
        )

      {^port, {:exit_status, status}} ->
        case status do
          0 -> {:ok, :turn_completed}
          _ -> {:error, {:port_exit, status}}
        end
    after
      timeout_ms ->
        {:error, :turn_timeout}
    end
  end

  defp handle_incoming(port, on_message, data, timeout_ms, workspace) do
    payload_string = to_string(data)

    case Jason.decode(payload_string) do
      {:ok, %{"type" => "completion"} = payload} ->
        emit_turn_event(on_message, :turn_completed, payload, payload_string, port, payload)
        {:ok, :turn_completed}

      {:ok, %{"type" => "error"} = payload} ->
        emit_turn_event(
          on_message,
          :turn_failed,
          payload,
          payload_string,
          port,
          payload
        )

        {:error, {:turn_failed, payload}}

      {:ok, %{"type" => "usage"} = payload} ->
        emit_message(
          on_message,
          :usage_update,
          %{
            payload: payload,
            raw: payload_string
          },
          metadata_from_message(port, payload)
        )

        receive_loop(port, on_message, timeout_ms, "", workspace)

      {:ok, payload} ->
        emit_message(
          on_message,
          :stream_message,
          %{
            payload: payload,
            raw: payload_string
          },
          metadata_from_message(port, payload)
        )

        receive_loop(port, on_message, timeout_ms, "", workspace)

      {:error, _reason} ->
        log_non_json_stream_line(payload_string, "stream")

        emit_message(
          on_message,
          :malformed,
          %{
            payload: payload_string,
            raw: payload_string
          },
          metadata_from_message(port, %{raw: payload_string})
        )

        receive_loop(port, on_message, timeout_ms, "", workspace)
    end
  end

  defp emit_turn_event(on_message, event, payload, payload_string, port, payload_details) do
    emit_message(
      on_message,
      event,
      %{
        payload: payload,
        raw: payload_string,
        details: payload_details
      },
      metadata_from_message(port, payload)
    )
  end

  defp log_non_json_stream_line(data, stream_label) do
    text =
      data
      |> to_string()
      |> String.trim()
      |> String.slice(0, @max_stream_log_bytes)

    if text != "" do
      if String.match?(text, ~r/\b(error|warn|warning|failed|fatal|panic|exception)\b/i) do
        Logger.warning("Claude Code #{stream_label} output: #{text}")
      else
        Logger.debug("Claude Code #{stream_label} output: #{text}")
      end
    end
  end

  defp issue_context(%{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp stop_port(port) when is_port(port) do
    case :erlang.port_info(port) do
      :undefined ->
        :ok

      _ ->
        try do
          Port.close(port)
          :ok
        rescue
          ArgumentError ->
            :ok
        end
    end
  end

  defp emit_message(on_message, event, details, metadata) when is_function(on_message, 1) do
    message = metadata |> Map.merge(details) |> Map.put(:event, event) |> Map.put(:timestamp, DateTime.utc_now())
    on_message.(message)
  end

  defp metadata_from_message(port, payload) do
    port |> port_metadata() |> maybe_set_usage(payload)
  end

  defp maybe_set_usage(metadata, payload) when is_map(payload) do
    usage = Map.get(payload, "usage") || Map.get(payload, :usage)

    if is_map(usage) do
      Map.put(metadata, :usage, usage)
    else
      metadata
    end
  end

  defp maybe_set_usage(metadata, _payload), do: metadata

  defp default_on_message(_message), do: :ok

  defp generate_session_id, do: "claude-#{System.unique_integer([:positive])}"
  defp generate_thread_id, do: "thread-#{System.unique_integer([:positive])}"
  defp generate_turn_id, do: "turn-#{System.unique_integer([:positive])}"
end