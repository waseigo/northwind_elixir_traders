defmodule NorthwindElixirTraders.Employee do
  use Ecto.Schema
  import Ecto.Changeset
  alias NorthwindElixirTraders.{Order, Validations}

  @name_mxlen 50
  @notes_mxlen 500

  schema "employees" do
    field(:last_name, :string)
    field(:first_name, :string)
    field(:birth_date, :date)
    field(:photo, :string)
    field(:notes, :string)
    has_many(:orders, Order, on_replace: :nilify)

    timestamps(type: :utc_datetime)
  end

  def changeset(data, params \\ %{}) do
    permitted = [:id, :last_name, :first_name, :birth_date, :photo, :notes]
    required = permitted |> List.delete(:id)

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:last_name, max: @name_mxlen)
    |> validate_length(:first_name, max: @name_mxlen)
    |> validate_length(:notes, max: @notes_mxlen)
    |> Validations.validate_age_range(:birth_date, min: 15, max: 100)
    |> unique_constraint([:last_name, :first_name, :birth_date])
    |> unique_constraint([:id])
  end
end
