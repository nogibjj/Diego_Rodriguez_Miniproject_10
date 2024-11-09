[![CI](https://github.com/nogibjj/Diego_Rodriguez_Miniproject_10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Diego_Rodriguez_Miniproject_10/actions/workflows/cicd.yml)

# IDS706-Miniproject #10
## File Structure 
```
Diego_Rodriguez_Mini_Project#10/
├── .devcontainer/
│   ├── devcontainer.json
│   └── Dockerfile
├── .github/
|   └── workflows/cicd.yml
├── data/
|   └── wdi.csv
├── mylib/
│   ├── __init__.py
|   └── lib.py
├── .gitignore
├── .ruff.toml
├── Dockerfile
├── LICENSE
├── main.py
├── Makefile
├── Pyspark_Result.md
├── README.md
├── repeat.sh
├── requirements.txt
├── setup.sh
└── test_main.py
```
## Purpose of project
The purpose of this project is to present some transformation of data using `PySpark` with World Bank data and automating the publishing the SQL query into `Pyspark_Result.md` with CI/CD. The data used in this repository corresponde to the World Development Indicator from the World Bank. 

###  PySpark Operations
Under `mylib/` directory `lib.py` performs pyspark uses suchs as:
  1. `extract` -> Take out the .csv file into a binary file to perform operations with PySpark.
  2. `load_data` -> Take a subset of the first 7 column of the database.
  3. `describe` -> Perfomrn statistical summary of the variable.
  4. `example_transform` -> Create a new column, subsetting countries from the South America region to a new variable `Country_Category`

###  Example Output

The operation is query data

The query is SELECT * FROM wdi WHERE country = 'Chile'

|    | country   |   fertility_rate |   viral |   battle |   cpia_1 |   cpia_2 |   debt |
|---:|:----------|-----------------:|--------:|---------:|---------:|---------:|-------:|
|  0 | Chile     |           47.526 |      86 |      nan |      nan |      nan |    nan |

For other operations , please check `Pyspark_Result.md`.

###  Testing and Lint Results:

<img width="1340" alt="Testing_lint" src="https://github.com/user-attachments/assets/09cc91bd-b71a-44c2-8f1e-c547dc2c841f">


