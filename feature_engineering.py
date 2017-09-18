

def replace_func(df,column_name,value):
    if value=="min":
        df = df.na.fill(df.select(column_name).na.drop().agg(min(column_name)).first()[0],[column_name])
    elif value=="avg":
        df = df.na.fill(df.select(column_name).na.drop().agg(avg(column_name)).first()[0], [column_name])
    elif value=="max":
        df = df.na.fill(df.select(column_name).na.drop().agg(max(column_name)).first()[0], [column_name])
    elif value=="mode":
        temp_df = df.select(column_name).na.drop()
        cnts = temp_df.groupBy(column_name).count()
        max_count = cnts.select("count").na.drop().agg(max("count")).first()[0]
        mode_val = cnts.filter(cnts["count"]==max_count).first()[0]
        df = df.na.fill(mode_val, [column_name])
    elif value == "median":
        median_value = df.select(column_name).na.drop().approxQuantile(column_name, [0.5], 0)
        df = df.na.fill(median_value, [column_name])
    else:
        df = df.na.fill(value, [column_name])
    return df


