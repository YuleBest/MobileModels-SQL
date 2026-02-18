import pandas as pd
import requests
from io import BytesIO
import os

def main():
    csv_url = "https://raw.githubusercontent.com/KHwang9883/MobileModels-csv/refs/heads/main/models.csv"
    sql_file = "update.sql"
    
    print(f"正在从 {csv_url} 获取数据...")
    try:
        res = requests.get(csv_url, timeout=30)
        res.raise_for_status()
        
        # 读取 CSV
        df = pd.read_csv(BytesIO(res.content))
        
        with open(sql_file, "w", encoding="utf-8") as f:
            # 基础设置：清空旧表并重新创建
            f.write("-- 自动生成的 SQL，请勿手动修改\n")
            f.write("DROP TABLE IF EXISTS phone_models;\n")
            f.write("CREATE TABLE phone_models (model TEXT, dtype TEXT, brand TEXT, brand_title TEXT, code TEXT, code_alias TEXT, model_name TEXT, ver_name TEXT);\n")
            
            # 使用事务加速插入
            f.write("BEGIN TRANSACTION;\n")
            
            for _, row in df.iterrows():
                # 处理 SQL 转义：将单引号 ' 替换为 ''，并处理空值
                items = []
                for val in row:
                    if pd.isnull(val):
                        items.append("NULL")
                    else:
                        safe_val = str(val).replace("'", "''")
                        items.append(f"'{safe_val}'")
                
                f.write(f"INSERT INTO phone_models VALUES ({', '.join(items)});\n")
                
            f.write("COMMIT;\n")
            
        print(f"成功生成 {sql_file}，共 {len(df)} 行数据。")
        
    except Exception as e:
        print(f"出错啦: {e}")
        exit(1)

if __name__ == "__main__":
    main()
