import pandas as pd


df = pd.DataFrame({'A': [1, 2, 3,4],'B': [400, 500, 600,700]})
new_df = pd.DataFrame({'B': [4, 5, 6], 'C': [7, 8, 9]})
df.update(new_df)
print(df)