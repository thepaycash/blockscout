defmodule Explorer.Repo.Migrations.InternalTransactionsAddToAddressHashIndex do
  use Ecto.Migration

  def change do

    create(index(:internal_transactions, :to_address_hash))

    drop_if_exists(
      index(:internal_transactions, [:to_address_hash, :from_address_hash, :created_contract_address_hash, :type, :index],
        name: "internal_transactions_to_address_hash_from_address_hash_created"
      )
    )

  end
end
