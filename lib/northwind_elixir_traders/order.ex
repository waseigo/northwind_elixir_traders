defmodule NorthwindElixirTraders.Order do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Customer, Employee, Shipper, Validations}

  schema "orders" do
    field(:date, :utc_datetime)
    belongs_to(:customer, Customer)
    belongs_to(:employee, Employee)
    belongs_to(:shipper, Shipper)

    timestamps(type: :utc_datetime)
  end

  def changeset(data, params \\ %{}) do
    permitted = [:id, :date, :customer_id, :employee_id, :shipper_id]
    required = permitted |> List.delete(:id)

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> Validations.validate_foreign_key_id(Customer, :customer_id)
    |> Validations.validate_foreign_key_id(Employee, :employee_id)
    |> Validations.validate_foreign_key_id(Shipper, :shipper_id)
    |> unique_constraint([:customer_id, :employee_id, :shipper_id, :date])
    |> unique_constraint([:id])
  end
end
