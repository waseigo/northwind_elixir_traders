alias NorthwindElixirTraders.{Repo,Category,Employee,Supplier,Product,Validations,DataImporter,Shipper,PhoneNumbers,Country,Customer,Order}

import Ecto.{Query,Changeset}

IEx.configure(inspect: [charlists: :as_lists])

url = "https://raw.githubusercontent.com/datasets/country-codes/2ed03b6993e817845c504ce9626d519376c8acaa/data/country-codes.csv"
