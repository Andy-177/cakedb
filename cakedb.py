import struct
import os
import json
import base64
from typing import Callable
from functools import wraps
from enum import Enum

# ==============================
# 类型常量
# ==============================
TYPE_NULL = 0x00
TYPE_BOOL = 0x01
TYPE_INT = 0x02
TYPE_FLOAT = 0x03
TYPE_STRING = 0x04
TYPE_ARRAY = 0x05
TYPE_OBJECT = 0x06
TYPE_BLOB = 0x07
TYPE_DATE = 0x08
TYPE_DECIMAL = 0x09
TYPE_UINT = 0x0A
TYPE_MARK = 0x0B  # 新增标记类型

# ==============================
# 标记常量
# ==============================
MARK_EMPTY = 0x0000      # 空标记
MARK_TAG = 0x0001        # tag标签

# ==============================
# CakeUtils 工具类
# ==============================
class CakeUtils:
    # 锁定模式枚举
    class LockMode(Enum):
        ALL = "all"
        KEY = "key"
        VALUE = "value"

    @staticmethod
    def autoexp(path=None):
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(db, *args, **kwargs):
                try:
                    res = func(db, *args, **kwargs)
                    success_msgs = ("insert success", "update success", "delete success",
                                    "clear success", "imp success", "j2d success")
                    if isinstance(res, str) and any(res.startswith(s) for s in success_msgs):
                        if path is None:
                            db.exp()
                        else:
                            db.exp(path)
                    return res
                except Exception as e:
                    return f"autoexp error:{str(e)}"
            return wrapper
        if callable(path):
            return decorator(path)
        return decorator

    @staticmethod
    def dbtypelock(mode=None):
        # 无参数装饰器：@CakeUtils.dbtypelock → 默认 ALL
        if callable(mode):
            func = mode
            utils = CakeUtils
            return utils.dbtypelock(utils.LockMode.ALL)(func)

        # 有参数装饰器：@CakeUtils.dbtypelock(...)
        mode = mode or CakeUtils.LockMode.ALL
        if isinstance(mode, CakeUtils.LockMode):
            mode_val = mode.value
        else:
            mode_val = str(mode).lower()

        def decorator(func: Callable):
            @wraps(func)
            def wrapper(db, *args, **kwargs):
                try:
                    db._check_imported()
                except Exception as e:
                    return f"dbtypelock error:{str(e)}"

                try:
                    fname = func.__name__
                    if len(args) < 1:
                        return func(db, *args, **kwargs)
                    key = args[0]
                    val = args[1] if len(args) > 1 else None

                    if fname in ("insert", "update") and key in db.data:
                        old_val = db.data[key]
                        lock_val = mode_val in ("all", "value")
                        lock_key = mode_val in ("all", "key")

                        if lock_val and val is not None:
                            t_old = db._auto_type(old_val)[0]
                            t_new = db._auto_type(val)[0]
                            if t_old != t_new:
                                return f"{fname} error:value type mismatch"

                        if lock_key:
                            if db.data:
                                keys = list(db.data.keys())
                                first_type = type(keys[0])
                                if not all(type(k) == first_type for k in keys):
                                    return f"{fname} error:inconsistent key types"
                                if type(key) != first_type:
                                    return f"{fname} error:key type mismatch"
                            else:
                                if not hasattr(db, '_key_type'):
                                    db._key_type = type(key)
                                else:
                                    if type(key) != db._key_type:
                                        return f"{fname} error:key type mismatch"
                except Exception:
                    pass

                try:
                    return func(db, *args, **kwargs)
                except Exception as e:
                    return f"{fname} error:{str(e)}"
            return wrapper
        return decorator

# ==============================
# CakeWriter
# ==============================
class CakeWriter:
    def __init__(self):
        self.buf = bytearray()

    def write(self, data: bytes):
        self.buf.extend(data)

    def write_null(self):
        self.write(struct.pack("B", TYPE_NULL))
        self.write(struct.pack("<I", 0))

    def write_bool(self, v: bool):
        self.write(struct.pack("B", TYPE_BOOL))
        self.write(struct.pack("<I", 1))
        self.write(struct.pack("B", 1 if v else 0))

    def write_int(self, v: int):
        self.write(struct.pack("B", TYPE_INT))
        self.write(struct.pack("<I", 8))
        self.write(struct.pack("<q", v))

    def write_uint(self, v: int):
        self.write(struct.pack("B", TYPE_UINT))
        self.write(struct.pack("<I", 8))
        self.write(struct.pack("<Q", v))

    def write_float(self, v: float):
        self.write(struct.pack("B", TYPE_FLOAT))
        self.write(struct.pack("<I", 8))
        self.write(struct.pack("<d", v))

    def write_string(self, s: str):
        b = s.encode("utf-8")
        self.write(struct.pack("B", TYPE_STRING))
        self.write(struct.pack("<I", len(b)))
        self.write(b)

    def write_blob(self, b: bytes):
        self.write(struct.pack("B", TYPE_BLOB))
        self.write(struct.pack("<I", len(b)))
        self.write(b)

    def write_date(self, ts: int):
        self.write(struct.pack("B", TYPE_DATE))
        self.write(struct.pack("<I", 8))
        self.write(struct.pack("<Q", ts))

    def write_decimal(self, integer: int, fractional: int):
        self.write(struct.pack("B", TYPE_DECIMAL))
        self.write(struct.pack("<I", 16))
        self.write(struct.pack("<q", integer))
        self.write(struct.pack("<Q", fractional))

    def write_array(self, elements):
        self.write(struct.pack("B", TYPE_ARRAY))
        temp = CakeWriter()
        for elem in elements:
            temp.write_any(elem)
        body = temp.buf
        self.write(struct.pack("<I", 4 + len(body)))
        self.write(struct.pack("<I", len(elements)))
        self.write(body)

    def write_object(self, kvs):
        self.write(struct.pack("B", TYPE_OBJECT))
        temp = CakeWriter()
        for item in kvs:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                k, v = item
                if isinstance(k, (list, tuple)) and len(k) == 2 and k[0] in range(0x0C):
                    temp.write_any(k)
                else:
                    temp.write_any(CakeDB._static_auto_type(k))
                
                if isinstance(v, (list, tuple)) and len(v) == 2 and v[0] in range(0x0C):
                    temp.write_any(v)
                else:
                    temp.write_any(CakeDB._static_auto_type(v))
            else:
                raise ValueError("object item must be (key, value) pair")
        body = temp.buf
        self.write(struct.pack("<I", 4 + len(body)))
        self.write(struct.pack("<I", len(kvs)))
        self.write(body)

    def write_mark(self, mark_const: int, metadata: bytes, real_tv):
        """
        写入标记类型
        :param mark_const: 标记常量(2字节)
        :param metadata: 二进制元数据
        :param real_tv: 真实数据的(type, value)
        """
        meta_len = len(metadata)
        self.write(struct.pack("B", TYPE_MARK))
        # 先写入标记常量、元数据长度、元数据
        self.write(struct.pack("<H", mark_const))
        self.write(struct.pack("<I", meta_len))
        if meta_len > 0:
            self.write(metadata)
        # 再写入真实数据
        self.write_any(real_tv)

    def write_any(self, tv):
        t, v = tv
        if t == TYPE_NULL:
            self.write_null()
        elif t == TYPE_BOOL:
            self.write_bool(v)
        elif t == TYPE_INT:
            self.write_int(v)
        elif t == TYPE_UINT:
            self.write_uint(v)
        elif t == TYPE_FLOAT:
            self.write_float(v)
        elif t == TYPE_STRING:
            self.write_string(v)
        elif t == TYPE_BLOB:
            self.write_blob(v)
        elif t == TYPE_DATE:
            self.write_date(v)
        elif t == TYPE_DECIMAL:
            if not isinstance(v, (list, tuple)) or len(v) != 2:
                raise ValueError("decimal must be (int, int)")
            self.write_decimal(v[0], v[1])
        elif t == TYPE_ARRAY:
            self.write_array(v)
        elif t == TYPE_OBJECT:
            self.write_object(v)
        elif t == TYPE_MARK:
            # v 应该是 (mark_const, metadata, real_tv)
            if not isinstance(v, (list, tuple)) or len(v) != 3:
                raise ValueError("mark must be (mark_const, metadata, real_tv)")
            self.write_mark(v[0], v[1], v[2])
        else:
            raise ValueError(f"invalid type {t}")

    def build_file(self, root_tv):
        self.write(b"cake")
        body = CakeWriter()
        body.write_any(root_tv)
        b = body.buf
        self.write(struct.pack("<Q", len(b)))
        self.write(b)
        return self.buf

# ==============================
# CakeReader
# ==============================
class CakeReader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def read(self, size: int):
        res = self.data[self.pos:self.pos+size]
        self.pos += size
        return res

    def read_any(self):
        type_id = struct.unpack("B", self.read(1))[0]
        length = struct.unpack("<I", self.read(4))[0]
        payload = self.read(length)
        r = CakeReader(payload)

        if type_id == TYPE_NULL:
            return None
        if type_id == TYPE_BOOL:
            return payload[0] == 1
        if type_id == TYPE_INT:
            return struct.unpack("<q", payload)[0]
        if type_id == TYPE_UINT:
            return struct.unpack("<Q", payload)[0]
        if type_id == TYPE_FLOAT:
            return struct.unpack("<d", payload)[0]
        if type_id == TYPE_STRING:
            return payload.decode("utf-8")
        if type_id == TYPE_BLOB:
            return payload
        if type_id == TYPE_DATE:
            return struct.unpack("<Q", payload)[0]
        if type_id == TYPE_DECIMAL:
            return (struct.unpack("<q", payload[:8])[0], struct.unpack("<Q", payload[8:])[0])

        if type_id == TYPE_ARRAY:
            cnt = struct.unpack("<I", r.read(4))[0]
            arr = [r.read_any() for _ in range(cnt)]
            if r.pos != len(payload):
                raise ValueError("array length mismatch")
            return arr

        if type_id == TYPE_OBJECT:
            cnt = struct.unpack("<I", r.read(4))[0]
            obj = {}
            for _ in range(cnt):
                k = r.read_any()
                v = r.read_any()
                obj[k] = v
            if r.pos != len(payload):
                raise ValueError("object length mismatch")
            return obj

        if type_id == TYPE_MARK:
            # 读取标记常量
            mark_const = struct.unpack("<H", self.read(2))[0]
            # 读取元数据长度
            meta_len = struct.unpack("<I", self.read(4))[0]
            # 读取元数据
            metadata = self.read(meta_len) if meta_len > 0 else b""
            # 读取真实数据
            real_value = self.read_any()
            # 返回带标记信息的数据结构
            return {"__mark__": True, "const": mark_const, "metadata": metadata, "value": real_value}

        return payload

    def read_file(self):
        if self.read(4) != b"cake":
            raise ValueError("invalid cake")
        data_len = struct.unpack("<Q", self.read(8))[0]
        start = self.pos
        res = self.read_any()
        if self.pos - start != data_len:
            raise ValueError("data length mismatch")
        return res
    
# ==============================
# Marker 标记管理类
# ==============================
class Marker:
    def __init__(self, db):
        self.db = db
    def rmk(self, key):
        """
        移除键对应的mark部分，只保留真实数据
        :param key: 键名
        :return: 成功返回 "rmk success:键"，失败返回错误信息
        """
        try:
            self.db._check_imported()
            if key not in self.db.data:
                return f"rmk error:key not found"
            
            value = self.db.data[key]
            if not CakeTag._is_marked(value):
                return f"rmk error:key has no mark"
            
            # 移除标记，只保留真实值
            self.db.data[key] = value["value"]
            self.db.root = self.db._auto_type(self.db.data)
            return f"rmk success:{key}"
        except Exception as e:
            return f"rmk error:{str(e)}"
# ==============================
# CakeTag 标记操作类
# ==============================
class CakeTag:
    def __init__(self, db):
        self.db = db
    @staticmethod
    def _is_marked(value):
        """检查值是否带有标记"""
        return isinstance(value, dict) and value.get("__mark__", False)

    @staticmethod
    def _unwrap_mark(value):
        """提取标记中的真实值"""
        if CakeTag._is_marked(value):
            return value["value"]
        return value

    @staticmethod
    def tagger(db, key: str, metadata: bytes = b""):
        """
        给指定键打上tag标签
        :param db: CakeDB实例
        :param key: 键名
        :param metadata: 二进制元数据
        :return: 成功返回 "tagger success:键"，失败返回错误信息
        """
        try:
            db._check_imported()
            if key not in db.data:
                return f"tagger error:key not found"
            
            value = db.data[key]
            
            # 创建标记包装
            marked_value = {
                "__mark__": True,
                "const": MARK_TAG,
                "metadata": metadata if isinstance(metadata, bytes) else metadata.encode("utf-8"),
                "value": value
            }
            
            db.data[key] = marked_value
            db.root = db._auto_type(db.data)
            return f"tagger success:{key}"
        except Exception as e:
            return f"tagger error:{str(e)}"

    @staticmethod
    def gettag(db, key: str):
        """
        获取指定键的tag标签的二进制元数据
        :param db: CakeDB实例
        :param key: 键名
        :return: 成功返回二进制元数据，失败返回错误信息
        """
        try:
            db._check_imported()
            if key not in db.data:
                return f"gettag error:key not found"
            
            value = db.data[key]
            if not CakeTag._is_marked(value):
                return f"gettag error:key has no tag mark"
            
            if value["const"] != MARK_TAG:
                return f"gettag error:mark is not a tag type"
            
            return value["metadata"]
        except Exception as e:
            return f"gettag error:{str(e)}"

# ==============================
# CakeDB
# ==============================
class CakeDB:
    def __init__(self):
        self.current_file = None
        self.root = None
        self.data = None
        self._key_type = None
        self.caketag = None
        self.marker = None
        self._init_caketag()
        self._init_marker()

    def _init_caketag(self):
        """初始化 tag 管理器"""
        if self.caketag is None:
            self.caketag = CakeTag(self)

    def _init_marker(self):
        """初始化 marker 管理器"""
        if self.marker is None:
            self.marker = Marker(self)

    def createdb(self, path):
        """
        创建一个空的数据库文件
        :param path: 必选参数，支持绝对和相对路径
        :return: 成功返回 "createdb success:路径"，失败返回错误信息
        """
        try:
            # 检查参数
            if path is None or path == "":
                return "createdb error:missing param"
            
            # 获取绝对路径
            p = os.path.abspath(path)
            
            # 检查文件是否已存在
            if os.path.exists(p):
                return f"createdb error:file already exists:{p}"
            
            # 创建空数据库对象（空字典）
            empty_obj = {}
            root_tv = self._auto_type(empty_obj)
            
            # 创建目录（如果不存在）
            d = os.path.dirname(p)
            if d:
                os.makedirs(d, exist_ok=True)
            
            # 写入空数据库文件
            buf = CakeWriter().build_file(root_tv)
            with open(p, "wb") as f:
                f.write(buf)
            
            return f"createdb success:{p}"
            
        except Exception as e:
            return f"createdb error:{str(e)}"

    @staticmethod
    def _static_auto_type(v):
        if v is None:
            return (TYPE_NULL, None)
        if isinstance(v, bool):
            return (TYPE_BOOL, v)
        if isinstance(v, int):
            return (TYPE_UINT, v) if v >= 0 else (TYPE_INT, v)
        if isinstance(v, float):
            return (TYPE_FLOAT, v)
        if isinstance(v, str):
            return (TYPE_STRING, v)
        if isinstance(v, bytes):
            return (TYPE_BLOB, v)
        if isinstance(v, (list, tuple)):
            return (TYPE_ARRAY, [CakeDB._static_auto_type(x) for x in v])
        if isinstance(v, dict):
            # 检查是否是标记包装
            if v.get("__mark__", False):
                # 标记类型
                real_tv = CakeDB._static_auto_type(v["value"])
                return (TYPE_MARK, (v["const"], v["metadata"], real_tv))
            # 普通对象
            items = [(k, CakeDB._static_auto_type(val)) for k, val in v.items()]
            return (TYPE_OBJECT, items)
        raise TypeError(f"unsupported type: {type(v)}")

    def _auto_type(self, v):
        return CakeDB._static_auto_type(v)

    def _check_imported(self):
        if self.root is None or self.data is None:
            raise Exception("not imported")

    def imp(self, path):
        try:
            p = os.path.abspath(path)
            with open(p, "rb") as f:
                data = f.read()
            obj = CakeReader(data).read_file()
            self.root = self._auto_type(obj)
            self.data = obj
            self.current_file = p
            self._key_type = None
            return f"imp success:{p}"
        except Exception as e:
            return f"imp error:{str(e)}"

    def exp(self, path=None):
        try:
            self._check_imported()
            if path is None and self.current_file is None:
                return "exp error:no file opened"
            p = os.path.abspath(path or self.current_file)
            d = os.path.dirname(p)
            if d:
                os.makedirs(d, exist_ok=True)
            buf = CakeWriter().build_file(self.root)
            tmp = p + ".tmp"
            with open(tmp, "wb") as f:
                f.write(buf)
            os.replace(tmp, p)
            return f"exp success:{p}"
        except Exception as e:
            return f"exp error:{str(e)}"

    def j2d(self, jpath, out=None):
        try:
            jp = os.path.abspath(jpath)
            op = out or (os.path.splitext(jpath)[0] + ".ck")
            op = os.path.abspath(op)
            with open(jp, "r", encoding="utf-8") as f:
                obj = json.load(f)
            self.root = self._auto_type(obj)
            self.data = obj
            self.current_file = op
            self._key_type = None
            result = self.exp(op)
            if result.startswith("exp success"):
                return f"j2d success:{op}"
            return result
        except Exception as e:
            return f"j2d error:{str(e)}"

    def _to_json_safe(self, obj):
        if isinstance(obj, bytes):
            return base64.b64encode(obj).decode()
        if isinstance(obj, (list, tuple)):
            return [self._to_json_safe(x) for x in obj]
        if isinstance(obj, dict):
            # 如果是标记包装，转换为可读格式
            if obj.get("__mark__", False):
                return {
                    "__mark__": True,
                    "const": obj["const"],
                    "metadata": base64.b64encode(obj["metadata"]).decode() if obj["metadata"] else "",
                    "value": self._to_json_safe(obj["value"])
                }
            return {k: self._to_json_safe(v) for k, v in obj.items()}
        return obj

    def d2j(self, out=None):
        try:
            self._check_imported()
            if not self.current_file:
                return "d2j error:no file opened"
            op = out or (os.path.splitext(self.current_file)[0] + ".json")
            op = os.path.abspath(op)
            safe_data = self._to_json_safe(self.data)
            os.makedirs(os.path.dirname(op), exist_ok=True)
            with open(op, "w", encoding="utf-8") as f:
                json.dump(safe_data, f, ensure_ascii=False, indent=2)
            return f"d2j success:{op}"
        except Exception as e:
            return f"d2j error:{str(e)}"

    def insert(self, key, val):
        try:
            self._check_imported()
            self.data[key] = val
            self.root = self._auto_type(self.data)
            return f"insert success:{key}"
        except Exception as e:
            return f"insert error:{str(e)}"

    def update(self, key, val):
        try:
            self._check_imported()
            if key not in self.data:
                return "update error:key not found"
            self.data[key] = val
            self.root = self._auto_type(self.data)
            return f"update success:{key}"
        except Exception as e:
            return f"update error:{str(e)}"

    def delete(self, key):
        try:
            self._check_imported()
            if key not in self.data:
                return "delete error:key not found"
            del self.data[key]
            self.root = self._auto_type(self.data)
            return f"delete success:{key}"
        except Exception as e:
            return f"delete error:{str(e)}"

    def clear(self):
        try:
            self._check_imported()
            self.data = {}
            self.root = self._auto_type(self.data)
            self._key_type = None
            return "clear success"
        except Exception as e:
            return f"clear error:{str(e)}"

    def select(self, key=None):
        try:
            self._check_imported()
            if key is None:
                return f"select success:{json.dumps(self._to_json_safe(self.data), ensure_ascii=False)}"
            if key not in self.data:
                return f"select error:key not found"
            return f"select success:{json.dumps(self._to_json_safe(self.data[key]), ensure_ascii=False)}"
        except Exception as e:
            return f"select error:{str(e)}"

    def count(self):
        try:
            self._check_imported()
            return f"count success:{len(self.data)}"
        except Exception as e:
            return f"count error:{str(e)}"

    def expo(self, key, path):
        """
        将键对应的对象导出为cko文件
        :param key: 要导出对象的对应键
        :param path: 保存路径
        :return: 成功返回 "expo success:路径"，失败返回错误信息
        """
        try:
            # 检查参数
            if key is None or path is None:
                return "expo error:missing param"
            
            # 检查数据库是否已导入
            self._check_imported()
            
            # 检查key是否存在
            if key not in self.data:
                return f"expo error:key not found"
            
            # 获取键对应的值
            value = self.data[key]
            
            # 获取值的类型
            value_tv = self._auto_type(value)
            
            # 获取绝对路径
            p = os.path.abspath(path)
            
            # 创建目录（如果不存在）
            d = os.path.dirname(p)
            if d:
                os.makedirs(d, exist_ok=True)
            
            # 构建cko文件
            # 魔数 "cko" + 值的完整原始数据
            buf = bytearray()
            buf.extend(b"cko")  # 魔数
            
            # 写入值的原始数据（支持任何类型）
            writer = CakeWriter()
            writer.write_any(value_tv)
            buf.extend(writer.buf)
            
            # 写入文件
            with open(p, "wb") as f:
                f.write(buf)
            
            return f"expo success:{p}"
            
        except Exception as e:
            return f"expo error:{str(e)}"

    def impo(self, key, path, forced=False):
        """
        挂载cko文件
        :param key: 要被挂载到的键
        :param path: cko文件的路径
        :param forced: 是否开启强制模式，默认False
        :return: 成功返回 "impo success:键"，失败返回错误信息
        """
        try:
            # 检查参数
            if key is None or path is None:
                return "impo error:missing param"
            
            # 检查数据库是否已导入
            self._check_imported()
            
            # 获取绝对路径
            p = os.path.abspath(path)
            
            # 检查文件是否存在
            if not os.path.exists(p):
                return f"impo error:file not found:{p}"
            
            # 读取cko文件
            with open(p, "rb") as f:
                data = f.read()
            
            # 检查魔数
            if len(data) < 3 or data[:3] != b"cko":
                return "impo error:invalid cko file format"
            
            # 解析值
            reader = CakeReader(data[3:])
            value = reader.read_any()
            
            # 验证解析是否完整
            if reader.pos != len(data[3:]):
                return "impo error:invalid cko file format"
            
            # 处理强制模式
            if not forced:
                # 非强制模式：检查key是否已存在且值不为空
                if key in self.data and self.data[key]:
                    # 对于不同的类型，判断是否为"空"
                    current_val = self.data[key]
                    is_empty = False
                    
                    if current_val is None:
                        is_empty = True
                    elif isinstance(current_val, (list, tuple)) and len(current_val) == 0:
                        is_empty = True
                    elif isinstance(current_val, dict) and len(current_val) == 0:
                        is_empty = True
                    elif isinstance(current_val, str) and current_val == "":
                        is_empty = True
                    elif isinstance(current_val, (int, float, bool)):
                        # 数值类型和布尔类型不视为空
                        is_empty = False
                    else:
                        is_empty = False
                    
                    if not is_empty:
                        return "impo error:value is not empty"
            
            # 插入或替换数据（支持任何类型）
            self.data[key] = value
            self.root = self._auto_type(self.data)
            
            return f"impo success:{key}"
            
        except Exception as e:
            return f"impo error:{str(e)}"