import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    with open('respiva.csv', 'r') as file:
        dataDict = dict()
        for line in file:
            line = line.strip()
            line = line.rstrip(',')
            line = line.split(',')
            curr_header = line[0]
            line = ','.join(line[1:])
            if curr_header in dataDict:
                dataDict[curr_header] += [line]
            else:
                dataDict[curr_header] = [line]
        max_length = max(len(l) for l in dataDict.values())

        for key, lst in dataDict.items():
            if len(lst) < max_length:
                lst.extend([None] * (max_length - len(lst)))

        df = pd.DataFrame(dataDict)

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write PyArrow Table to Parquet file
        output_file = 'output2.parquet'
        pq.write_table(table, output_file)

        print(f"Parquet file '{output_file}' has been written successfully.")


if __name__ == '__main__':
    main()
