defmodule NorthwindElixirTraders.Employee do
  @enforce_keys [:last_name, :first_name, :birth_date]
  defstruct [:last_name, :first_name, :birth_date, :photo, :notes]
end
