defmodule Indexer.Temporary.DoubleTokenTransfers do
  @moduledoc """
  Fetches in range all the Block numbers with transactions that have multiple
  token transfers and with these:
  - removes consensus from the block
  - deletes logs and token transfers of its transactions
  """

  use Indexer.Fetcher

  require Logger

  import Ecto.Query

  alias Ecto.Multi
  alias Explorer.{BlockRefetcher, Repo}
  alias Explorer.Chain.{Block, Log, TokenTransfer, Transaction}
  alias Indexer.BufferedTask

  @behaviour BufferedTask

  @fetcher_name :double_token_transfers

  @defaults [
    flush_interval: :timer.seconds(3),
    max_batch_size: 50,
    max_concurrency: 1,
    task_supervisor: Indexer.Temporary.DoubleTokenTransfers.TaskSupervisor,
    metadata: [fetcher: @fetcher_name]
  ]

  @doc false
  # credo:disable-for-next-line Credo.Check.Design.DuplicatedCode
  def child_spec([init_options, gen_server_options]) do
    merged_init_opts =
      @defaults
      |> Keyword.merge(init_options)
      |> Keyword.put(:state, {})

    Supervisor.child_spec({BufferedTask, [{__MODULE__, merged_init_opts}, gen_server_options]}, id: __MODULE__)
  end

  @impl BufferedTask
  def init(initial, reducer, _) do
    %{first_block_number: first, last_block_number: last} = get_starting_numbers()

    if BlockRefetcher.no_work_left(first, last) do
      {0, []}
    else
      transfers_query =
        from(
          t in TokenTransfer,
          group_by: [t.transaction_hash],
          having: count() > 1,
          select: t.transaction_hash
        )

      transactions_query =
        from(
          t in Transaction,
          join: s in subquery(transfers_query),
          on: t.hash == s.transaction_hash,
          distinct: true,
          select: t.block_number,
          order_by: [asc: t.block_number]
        )

      query =
        transactions_query
        |> where_first_block_number(first)
        |> where_last_block_number(last)

      {:ok, final} = Repo.stream_reduce(query, initial, &reducer.(&1, &2))

      final
    end
  end

  @impl BufferedTask
  def run(numbers, _) do
    # Enforce ShareLocks tables order (see docs: sharelocks.md)
    multi =
      Multi.new()
      |> Multi.run(:transaction_hashes, fn repo, _ ->
        query =
          from(
            t in Transaction,
            where: t.block_number in ^numbers,
            select: t.hash
          )

        {:ok, repo.all(query)}
      end)
      |> Multi.run(:remove_blocks_consensus, fn repo, _ ->
        query =
          from(
            block in Block,
            where: block.number in ^numbers,
            # Enforce Block ShareLocks order (see docs: sharelocks.md)
            order_by: [asc: block.hash],
            lock: "FOR UPDATE"
          )

        {_num, result} =
          repo.update_all(
            from(b in Block, join: s in subquery(query), on: b.hash == s.hash),
            set: [consensus: false]
          )

        {:ok, result}
      end)
      |> Multi.run(:remove_logs, fn repo, %{transaction_hashes: hashes} ->
        query =
          from(
            log in Log,
            where: log.transaction_hash in ^hashes,
            # Enforce Log ShareLocks order (see docs: sharelocks.md)
            order_by: [asc: log.transaction_hash, asc: log.index],
            lock: "FOR UPDATE"
          )

        {_num, result} =
          repo.delete_all(from(l in Log, join: s in subquery(query), on: l.transaction_hash == s.transaction_hash))

        {:ok, result}
      end)
      |> Multi.run(:remove_token_transfers, fn repo, %{transaction_hashes: hashes} ->
        query =
          from(
            transfer in TokenTransfer,
            where: transfer.transaction_hash in ^hashes,
            # Enforce TokenTransfer ShareLocks order (see docs: sharelocks.md)
            order_by: [asc: transfer.transaction_hash, asc: transfer.log_index],
            lock: "FOR UPDATE"
          )

        {_num, result} =
          repo.delete_all(
            from(tt in TokenTransfer, join: s in subquery(query), on: tt.transaction_hash == s.transaction_hash)
          )

        {:ok, result}
      end)
      |> Multi.run(:update_refetcher_status, fn repo, _ ->
        new_first = Enum.max(numbers)

        @fetcher_name
        |> BlockRefetcher.fetch()
        |> repo.one()
        |> BlockRefetcher.with_first(new_first)
        |> repo.update()

        {:ok, new_first}
      end)

    try do
      multi
      |> Repo.transaction()
      |> case do
        {:ok, _res} ->
          :ok

        {:error, _error} ->
          {:retry, numbers}
      end
    rescue
      postgrex_error in Postgrex.Error ->
        Logger.error(fn -> ["Error while handling double token transfers", inspect(postgrex_error)] end)
        {:retry, numbers}
    end
  end

  defp get_starting_numbers do
    @fetcher_name
    |> BlockRefetcher.fetch()
    |> Repo.one()
    |> case do
      nil ->
        @fetcher_name
        |> BlockRefetcher.make_from_env()
        |> Repo.insert!()

      value ->
        value
    end
  end

  defp where_first_block_number(query, number) when is_nil(number), do: query

  defp where_first_block_number(query, number) do
    where(query, [t], t.block_number > ^number)
  end

  defp where_last_block_number(query, number) when is_nil(number), do: query

  defp where_last_block_number(query, number) do
    where(query, [t], t.block_number <= ^number)
  end
end
