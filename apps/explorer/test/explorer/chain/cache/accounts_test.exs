defmodule Explorer.Chain.Cache.AccountsTest do
  use Explorer.DataCase

  alias Explorer.Chain.Cache.Accounts
  alias Explorer.Repo

  @size 51

  describe "drop_or_update/1" do
    test "adds a new value to a new cache with preloads" do
      address = insert(:address) |> preload_names()

      Accounts.update(address)

      assert Accounts.take(1) == [address]
    end

    test "updates an existing address if its fetched_coin_balance has not changed" do
      address =
        insert(:address, fetched_coin_balance: 100_000, fetched_coin_balance_block_number: 1)
        |> preload_names()

      Accounts.update(address)

      assert Accounts.take(1) == [address]

      updated_address = %{address | fetched_coin_balance: 100_001}

      Accounts.update(updated_address)

      assert Accounts.take(1) == [updated_address]
    end

    test "drops the cache if an address was in the cache with a different fetched_coin_balance" do
      address =
        insert(:address, fetched_coin_balance: 100_000, fetched_coin_balance_block_number: 1)
        |> preload_names()

      Accounts.update(address)

      assert Accounts.take(1) == [address]

      updated_address = %{address | fetched_coin_balance: 100_001}

      Accounts.update(updated_address)

      assert Accounts.take(1) == [updated_address]
    end
  end

  defp preload_names(address) do
    Repo.preload(address, [:names])
  end
end
