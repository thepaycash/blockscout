defmodule Explorer.Chain.Cache.Accounts do
  @moduledoc """
  Caches the top Addresses
  """

  alias Explorer.Chain.Address

  use Explorer.Chain.OrderedCache,
    name: :accounts,
    max_size: 51,
    preload: :names,
    ttl_check_interval: Application.get_env(:explorer, __MODULE__)[:ttl_check_interval],
    global_ttl: Application.get_env(:explorer, __MODULE__)[:global_ttl]

  @type element :: Address.t()

  @type id :: {non_neg_integer(), non_neg_integer()}

  def element_to_id(%Address{fetched_coin_balance: fetched_coin_balance, hash: hash}) do
    {fetched_coin_balance, hash}
  end

  def prevails?({fetched_coin_balance_a, hash_a}, {fetched_coin_balance_b, hash_b}) do
    # same as a query's `order_by: [desc: :fetched_coin_balance, asc: :hash]`
    if fetched_coin_balance_a == fetched_coin_balance_b do
      hash_a > hash_b
    else
      fetched_coin_balance_a < fetched_coin_balance_b
    end
  end

  def drop_or_update(nil), do: :ok

  def drop_or_update([]), do: :ok

  def drop_or_update(addresses) do
    # Note: because the fetched_coin_balance of each address may change constantly,
    # we cannot just let the Indexer `update` the cache.
    # The reason being that if an address coin balance update drops, we have no
    # way of knowing if it should stay in the cache or if there is another address in the
    # database that should take its place.
    # The only thing we can safely do when an address in the cache changes its
    # `fetched_coin_balance` is to invalidate the whole cache and wait for it
    # to be filled again.
    ids_map = Map.new(ids_list(), fn {balance, hash} -> {hash, balance} end)

    drop_needed =
      Enum.find_value(addresses, false, fn address ->
        stored_address_balance = Map.get(ids_map, address.hash)

        not is_nil(stored_address_balance) and stored_address_balance != address.fetched_coin_balance
      end)

    if drop_needed do
      ConCache.update(cache_name(), ids_list_key(), fn ids ->
        # Remove the addresses immediately
        Enum.each(ids, &ConCache.delete(cache_name(), &1))

        {:ok, []}
      end)
    else
      # filter addresses without fetched_coin_balance and update the cache as usual
      addresses
      |> Enum.filter(&(not is_nil(&1.fetched_coin_balance) and &1.fetched_coin_balance > 0))
      |> update()
    end
  end
end
