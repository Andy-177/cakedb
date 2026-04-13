# CakeDB 🍰

CakeDB是一款基于JSON的对象数据库，该数据库仿照JSON格式将JSON变成了二进制形式存储，也就是数据库

# 介绍

## JSON完整和结构无损

CakeDB的数据库文件是JSON的超集，在使用其自带的转换功能时可以实现原始JSON和转换后的JSON结构无损，也就是将原始文件和转换回来的JSON全部压缩成单行进行哈希值比较，结果是两边哈希值完全一致，这说明cakedb完整地保留了整个JSON的结构，转换回去的文件只有排版和原JSON不同，但是结构和原JSON完全一致，可以正常读取。

**使用下面的python代码可以测试是否结构完整：**
```
import json
from cakedb import CakeDB  # 假设上面的代码保存为 your_module.py

# 创建数据库实例
db = CakeDB()
# ========== 第2步：JSON → Cake 数据库 (j2d) ==========
result = db.j2d("input.json", out="output.ck")
print(f"📦 j2d 结果: {result}")
# 输出: j2d success:/path/to/output.ck

# ========== 第3步：Cake 数据库 → JSON (d2j) ==========
result = db.d2j(out="output.json")
print(f"📄 d2j 结果: {result}")
# 输出: d2j success:/path/to/output.json

# 验证结果
with open("output.json", "r", encoding="utf-8") as f:
    restored = json.load(f)
with open("input.json", "r", encoding="utf-8") as f:
    original = json.load(f)
print(f"✅ 转换完成！数据一致: {restored == original}")

```
