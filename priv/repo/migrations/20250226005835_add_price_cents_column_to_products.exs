defmodule NorthwindElixirTraders.Repo.Migrations.AddPriceCentsColumnToProducts do
  use Ecto.Migration

  def up do
    alter table(:products) do
      add(:price_cents, :integer)
    end

    execute("UPDATE products SET price_cents = CAST(price*100 AS INTEGER)")
  end

  def down do
    alter(table(:products), do: remove(:price_cents))
  end
end
