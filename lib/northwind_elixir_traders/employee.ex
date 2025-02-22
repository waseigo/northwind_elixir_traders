defmodule NorthwindElixirTraders.Employee do
  use Ecto.Schema
  import Ecto.Changeset

  @name_mxlen 50
  @notes_mxlen 500

  schema "employees" do
    field(:last_name, :string)
    field(:first_name, :string)
    field(:birth_date, :date)
    field(:photo, :string)
    field(:notes, :string)

    timestamps(type: :utc_datetime)
  end

  def changeset(data, params \\ %{}) do
    permitted = [:last_name, :first_name, :birth_date, :photo, :notes]
    required = [:last_name, :first_name, :birth_date]

    data
    |> cast(params, permitted)
    |> validate_required(required)
    |> validate_length(:last_name, max: @name_mxlen)
    |> validate_length(:first_name, max: @name_mxlen)
    |> validate_length(:notes, max: @notes_mxlen)
  end
end
