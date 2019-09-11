defmodule Indexer.Temporary.DoubleTokenTransfersTest do
  use Explorer.DataCase

  alias Explorer.BlockRefetcher
  alias Explorer.Chain.{Block, Log, TokenTransfer}
  alias Indexer.Temporary.DoubleTokenTransfers

  @fetcher_name :double_token_transfers

  describe "run/2" do
    setup do
      # clear the data from the database
      @fetcher_name
      |> BlockRefetcher.fetch()
      |> Repo.one()
      |> case do
        nil -> %BlockRefetcher{name: Atom.to_string(@fetcher_name)}
        refetcher -> refetcher
      end
      |> BlockRefetcher.changeset(%{first_block_number: nil, last_block_number: nil})
      |> Repo.insert_or_update()

      :ok
    end

    test "removes consensus from blocks with multiple token_transfers" do
      block = insert(:block)
      transaction = insert(:transaction) |> with_block(block)
      insert(:token_transfer, transaction: transaction)
      insert(:token_transfer, transaction: transaction)

      block_number = block.number

      DoubleTokenTransfers.run([block_number], nil)

      assert %{consensus: false} = from(b in Block, where: b.number == ^block_number) |> Repo.one()
    end

    test "deletes logs from transactions of blocks with multiple token_transfers" do
      block = insert(:block)
      transaction = insert(:transaction) |> with_block(block)
      insert(:token_transfer, transaction: transaction)
      insert(:token_transfer, transaction: transaction)

      insert(:log, transaction: transaction)
      insert(:log, transaction: transaction)

      assert 2 =
               from(l in Log, where: l.transaction_hash == ^transaction.hash)
               |> Repo.aggregate(:count, :transaction_hash)

      block_number = block.number

      DoubleTokenTransfers.run([block_number], nil)

      assert 0 =
               from(l in Log, where: l.transaction_hash == ^transaction.hash)
               |> Repo.aggregate(:count, :transaction_hash)
    end

    test "deletes token_transfers from transactions of blocks with multiple token_transfers" do
      block = insert(:block)
      transaction = insert(:transaction) |> with_block(block)
      insert(:token_transfer, transaction: transaction)
      insert(:token_transfer, transaction: transaction)

      assert 2 =
               from(t in TokenTransfer, where: t.transaction_hash == ^transaction.hash)
               |> Repo.aggregate(:count, :transaction_hash)

      block_number = block.number

      DoubleTokenTransfers.run([block_number], nil)

      assert 0 =
               from(t in TokenTransfer, where: t.transaction_hash == ^transaction.hash)
               |> Repo.aggregate(:count, :transaction_hash)
    end

    test "updates block_refetcher data after correcting a block" do
      block = insert(:block)
      transaction = insert(:transaction) |> with_block(block)
      insert(:token_transfer, transaction: transaction)
      insert(:token_transfer, transaction: transaction)

      insert(:log, transaction: transaction)
      insert(:log, transaction: transaction)

      assert %{first_block_number: nil} = @fetcher_name |> BlockRefetcher.fetch() |> Repo.one()

      block_number = block.number

      DoubleTokenTransfers.run([block_number], nil)

      assert %{first_block_number: block_number} = @fetcher_name |> BlockRefetcher.fetch() |> Repo.one()
    end
  end
end
