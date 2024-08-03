import pandas as pd
import numpy as np
import re
import urllib.parse
import sqlalchemy
from sqlalchemy import create_engine


class TransformData:

    def __init__(self, file_location, sheetname):

        # Read data from Excel
        self.df_data = pd.read_excel(file_location, sheet_name=sheetname)
        self.possible_formats = []
        # list of Months
        self.months = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)"
        self.cols_sql = []
        self.default_amount = [
            "amount",
            "sales",
            "quantity",
            "discount",
            "profit",
            "revenue",
            "cost",
        ]
        self.default_date = ["date"]
        self.Amt_col = []
        self.Date_col = []
        self.correct_date_format = ""

    def find_possible_date_formats(self, date_str):
        # Regular Expression for all date format
        patterns = {
            "YYYY-MM-DD": r"^([2-9][0-9]{3})-(1[0-2]|0?[1-9])-(3[01]|[12][0-9]|0?[1-9])$",
            "DD-MM-YYYY": r"^(3[01]|[12][0-9]|0?[1-9])-(1[0-2]|0?[1-9])-([2-9][0-9]{3})$",
            "MM-DD-YYYY": r"^(1[0-2]|0?[1-9])-(3[01]|[12][0-9]|0?[1-9])-([2-9][0-9]{3})$",
            "YYYY/MM/DD": r"^([2-9][0-9]{3})/(1[0-2]|0?[1-9])/(3[01]|[12][0-9]|0?[1-9])$",
            "DD/MM/YYYY": r"^(3[01]|[12][0-9]|0?[1-9])/(1[0-2]|0?[1-9])/([2-9][0-9]{3})$",
            "MM/DD/YYYY": r"^(1[0-2]|0?[1-9])/(3[01]|[12][0-9]|0?[1-9])/([2-9][0-9]{3})$",
            "YYYY.MM.DD": r"^([2-9][0-9]{3})\.(1[0-2]|0?[1-9])\.(3[01]|[12][0-9]|0?[1-9])$",
            "DD.MM.YYYY": r"^(3[01]|[12][0-9]|0?[1-9])\.(1[0-2]|0?[1-9])\.([2-9][0-9]{3})$",
            "MM.DD.YYYY": r"^(1[0-2]|0?[1-9])\.(3[01]|[12][0-9]|0?[1-9])\.([2-9][0-9]{3})$",
            "DD Month YYYY": rf"^(3[01]|[12][0-9]|0?[1-9]) {self.months} ([2-9][0-9]{{3}})$",
        }

        possible_formats = []
        for format_name, pattern in patterns.items():
            if re.match(pattern, date_str):
                possible_formats.append(format_name)

        return possible_formats

    def detect_date_format(self, date_col):
        Top100Date = (
            self.df_data.groupby(date_col).head(100).reset_index(drop=True)[date_col]
        )

        for date in Top100Date:
            self.possible_formats.extend(self.find_possible_date_formats(date))
        if self.possible_formats:
            correct_date_format = max(
                set(self.possible_formats), key=self.possible_formats.count
            )
            return correct_date_format
        else:
            return "No valid date format found"

    def transform_column_name(self, col):
        cleaned_col = re.sub(r"\s+", " ", re.sub(r"[^a-zA-Z\s]", "", col)).strip()
        cleaned_col = cleaned_col.replace(" ", "_")
        return col

    def detect_amount_date(self):
        # Rename all name of column to correct format
        j = 1
        for i in self.df_data.columns:
            cleaned_name = self.transform_column_name(i)
            if cleaned_name not in self.cols_sql:
                self.cols_sql.append(cleaned_name)
                self.df_data.rename(columns={i: cleaned_name}, inplace=True)
            else:
                new_name = cleaned_name + "_" + str(j)
                self.cols_sql.append(new_name)
                self.df_data.rename(columns={i: new_name}, inplace=True)
                j += 1

        # Determine all date and amount columns
        for i in self.df_data.columns:
            for j in self.default_amount:
                if j in self.transform_column_name(i).lower():
                    self.Amt_col.append(i)
            for j in self.default_date:
                if j in self.transform_column_name(i).lower():
                    self.Date_col.append(i)

        print("Amount Columns:", self.Amt_col)
        print("Date Columns:", self.Date_col)
        return None

    def cleaned_date_col(self, col, mode, delimiter):
        match mode:
            case 1:
                self.df_data[["Tom_month", "Tom_day", "Tom_year"]] = self.df_data[
                    col
                ].str.split(delimiter, expand=True)
            case 2:
                self.df_data[["Tom_day", "Tom_month", "Tom_year"]] = self.df_data[
                    col
                ].str.split(delimiter, expand=True)
            case 3:
                self.df_data[["Tom_year", "Tom_month", "Tom_day"]] = self.df_data[
                    col
                ].str.split(delimiter, expand=True)
        self.df_data["Tom_day"] = self.df_data["Tom_day"].astype(str)
        self.df_data["Tom_month"] = self.df_data["Tom_month"].astype(str)
        self.df_data["Tom_year"] = self.df_data["Tom_year"].astype(str)
        self.df_data[col] = (
            self.df_data["Tom_year"]
            + "-"
            + self.df_data["Tom_month"]
            + "-"
            + self.df_data["Tom_day"]
        )
        self.df_data.drop(columns=["Tom_day", "Tom_month", "Tom_year"], inplace=True)
        return self.df_data

    def transform_all_date_col(self):
        self.detect_amount_date()
        date_format = (
            self.detect_date_format(self.Date_col[0])
            if self.Date_col
            else "No valid date format found"
        )
        for i in self.Date_col:
            match date_format:
                case "YYYY-MM-DD":
                    self.cleaned_date_col(i, 3, "-")
                case "YYYY/MM/DD":
                    self.cleaned_date_col(i, 3, "/")
                case "YYYY.MM.DD":
                    self.cleaned_date_col(i, 3, ".")
                case "DD-MM-YYYY":
                    self.cleaned_date_col(i, 2, "-")
                case "DD/MM/YYYY":
                    self.cleaned_date_col(i, 2, "/")
                case "DD.MM.YYYY":
                    self.cleaned_date_col(i, 2, ".")
                case "MM-DD-YYYY":
                    self.cleaned_date_col(i, 1, "-")
                case "MM/DD/YYYY":
                    self.cleaned_date_col(i, 1, "/")
                case "MM.DD.YYYY":
                    self.cleaned_date_col(i, 1, ".")
        return self.df_data

    def transform_all_number_col(self):
        np.random.seed(42)
        for i in self.Amt_col:
            if i in self.df_data.columns:
                self.df_data[i + "_Tom_sample"] = (
                    self.df_data[i].sample(frac=1, random_state=42).values
                )
                random_factors = np.random.uniform(0.5, 1.5, self.df_data.shape[0])
                self.df_data[i] = self.df_data[i + "_Tom_sample"] * random_factors
                self.df_data.drop(columns=[i + "_Tom_sample"], inplace=True)
        return self.df_data

    def transform_data_type(self):
        for i in self.df_data.columns:
            if i not in self.Amt_col and i not in self.Date_col:
                self.df_data[i].astype(str)
            elif i in self.Amt_col:
                self.df_data[i].astype(float)
            elif i in self.Date_col:
                self.df_data[i] = pd.to_datetime(self.df_data[i], format="%d/%m/%Y")

    def result(self):
        self.detect_amount_date()
        self.transform_all_number_col()
        self.transform_data_type()

        return self.df_data


class MaskData:

    def __init__(self, df_data, cols_to_masked, cols_mask, path_masked_file):
        self.df_data = df_data
        self.cols_to_masked = cols_to_masked
        self.cols_mask = cols_mask
        self.path_masked_file = path_masked_file
        self.df_name = pd.read_csv(path_masked_file)

    def mask_data(self):
        for i in self.cols_to_masked:
            random_index = np.random.randint(
                0, self.df_name.shape[0], self.df_data.shape[0]
            )
            self.df_data[i] = self.df_name.iloc[random_index][self.cols_mask].values
        return self.df_data

    def generate_random_phone_number(self):
        area_code = np.random.randint(100, 999)
        exchange_code = np.random.randint(100, 999)
        subscriber_number = np.random.randint(1000, 9999)

        phone_number = f"({area_code}) {exchange_code}-{subscriber_number}"
        return phone_number

    def mask_phone(self):
        for i in self.cols_to_masked:
            phone_numbers = [
                self.generate_random_phone_number()
                for _ in range(self.df_data.shape[0])
            ]
            self.df_data[i] = phone_numbers
        return self.df_data


class CreateTableInSQLServer:
    def __init__(self, SQLServerName, DBName, TableName, df_data):
        self.connect_string = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "Server=" + SQLServerName + ";"
            "Database=" + DBName + ";"
            "Trusted_Connection=yes;"
        )
        self.TableName = TableName
        self.df_data = df_data

    def run(self):
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={self.connect_string}",
            fast_executemany=True,
        )
        with engine.connect() as connection:
            self.df_data.to_sql(
                self.TableName, connection, index=False, if_exists="append"
            )
            print("OK")


# class Generate_10500_OppName:
#     def __init__(self, loc_export):
#         self.loc_export = loc_export

#     def generate_fake_company_name():
#         prefixes = [
#             "Global",
#             "Advanced",
#             "Prime",
#             "Dynamic",
#             "Innovative",
#             "Quantum",
#             "Superior",
#             "Elite",
#             "United",
#             "Pioneer",
#         ]
#         suffixes = [
#             "Solutions",
#             "Corporation",
#             "Technologies",
#             "Industries",
#             "Enterprises",
#             "Consulting",
#             "Holdings",
#             "Group",
#             "Services",
#             "Partners",
#         ]
#         return f"{random.choice(prefixes)} {random.choice(suffixes)}"

#     def generate_data(self):

#         return None


# import pandas as pd
# import random
# import string

# # Function to generate a fake company name


# fake_company_names = [generate_fake_company_name() for _ in range(100)]
# # List of 10 states
# states = [
#     "US",
#     "UK",
#     "US",
#     "VN",
#     "Pennsylvania",
#     "Ohio",
#     "Georgia",
#     "North Carolina",
#     "Michigan",
# ]
# # Keywords related to accounting, audit, and tax
# accounting_keywords = ["Financial", "Ledger", "Balance", "Reporting", "Bookkeeping"]
# audit_keywords = ["Compliance", "Review", "Assessment", "Inspection", "Certification"]
# tax_keywords = ["Taxation", "Filing", "Returns", "Compliance", "Planning"]
# keywords = accounting_keywords + audit_keywords + tax_keywords


# # Function to generate a random opportunity name related to accounting, audit, and tax, with a fake company name
# def generate_related_opportunity_name():
#     keyword = random.choice(keywords)
#     company = random.choice(fake_company_names)

#     state = random.choice(states)
#     return f"{keyword} Opportunity in {state} at {company}"


# # Generate 10500 random opportunity names
# related_opportunity_names = [generate_related_opportunity_name() for _ in range(10500)]

# # Create a DataFrame
# df_related = pd.DataFrame(related_opportunity_names, columns=["Opportunity Name"])

# # Save to CSV
# csv_path_related = "accounting_audit_tax_opportunity_names_with_fake_companies.csv"
# df_related.to_csv(csv_path_related, index=False)

# csv_path_related
