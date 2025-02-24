defmodule NorthwindElixirTraders.PhoneNumbers do
  # @url "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv"
  # do not use this^
  # this is stable
  @url "https://raw.githubusercontent.com/datasets/country-codes/2ed03b6993e817845c504ce9626d519376c8acaa/data/country-codes.csv"

  @intl_regex ~r/^\+((?:9[679]|8[035789]|6[789]|5[90]|42|3[578]|2[1-689])|9[0-58]|8[1246]|6[0-6]|5[1-8]|4[013-9]|3[0-469]|2[70]|7|1)(?:\W*\d){0,13}\d$/
  @nanp_regex ~r/^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$/

  def is_intl?(phone) when is_bitstring(phone), do: Regex.match?(@intl_regex, phone)
  def is_nanp?(phone) when is_bitstring(phone), do: Regex.match?(@nanp_regex, phone)

  def get_csv_rows() do
    {status, result} = :httpc.request(@url)

    case {status, result} do
      {:ok, {_status, _headers, body}} ->
        {:ok,
         body
         |> List.to_string()
         |> String.trim()
         |> String.split("\n")
         |> Enum.map(&fix_csv_row/1)
         |> Enum.map(&String.split(&1, ","))}

      {:error, {status, _, _}} ->
        {:error, status}
    end
  end

  def fix_csv_row(row) when is_bitstring(row) do
    regex = ~r/"([^"]*)"/
    String.replace(row, regex, &(String.replace(&1, ",", "|") |> String.replace("\"", "")))
  end
end
