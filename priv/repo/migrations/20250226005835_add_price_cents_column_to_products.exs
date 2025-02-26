defmodule NorthwindElixirTraders.Repo.Migrations.AddPriceCentsColumnToProducts do
  use Ecto.Migration

  def change do
    alter table(:products) do
      add(:price_cents, :integer)
    end

    execute("UPDATE products SET price_cents = price*100")
  end
end
