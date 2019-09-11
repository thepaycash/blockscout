defmodule Explorer.Repo.Migrations.CreateBlockRefetchers do
  use Ecto.Migration

  def change do
    create table(:block_refetchers, primary_key: false) do
      add(:name, :string, primary_key: true)
      add(:first_block_number, :bigint, null: true)
      add(:last_block_number, :bigint, null: true)

      timestamps(null: false, type: :utc_datetime_usec)
    end
  end
end
