defmodule NorthwindElixirTraders.Repo.Migrations.EmployeesUniqueIndex do
  use Ecto.Migration

  def change do
    create unique_index(:employees, [:last_name, :first_name, :birth_date])
  end
end
