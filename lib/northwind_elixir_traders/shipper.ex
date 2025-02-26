defmodule NorthwindElixirTraders.Shipper do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Order, PhoneNumbers}

  @name_mxlen 50

  schema "shippers" do
    field(:name, :string)
    field(:phone, :string)
    has_many(:orders, Order, on_replace: :nilify)

    timestamps(type: :utc_datetime)
  end

  def import_changeset(data, params \\ %{}) do
    permitted = [:id, :name, :phone]
    required = permitted

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:name, max: @name_mxlen)
    |> PhoneNumbers.validate_phone(:phone)
    |> unique_constraint([:name])
    |> unique_constraint([:id])
  end
end
