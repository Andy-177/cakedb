import struct
import os
import json
import base64
from typing import Any, Callable
from functools import wraps

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

# ==============================
# Writer
# ==============================
class CakeWriter:
    def __init__(self):
        self.buf = bytearray()

    def write(self, data: bytes):
        self.buf.extend(data)

    def write_null(self):
        self.write(struct.pack("B", TYPE_NULL))

    def write_bool(self, v: bool):
        self.write(struct.pack("B", TYPE_BOOL))
        self.write(struct.pack("B", 1 if v else 0))

    def write_int(self, v: int):
        self.write(struct.pack("B", TYPE_INT))
        self.write(struct.pack("<q", v))

    def write_uint(self, v: int):
        self.write(struct.pack("B", TYPE_UINT))
        self.write(struct.pack("<Q", v))

    def write_float(self, v: float):
        self.write(struct.pack("B", TYPE_FLOAT))
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
        self.write(struct.pack("<Q", ts))

    def write_decimal(self, integer: int, fractional: int):
        self.write(struct.pack("B", TYPE_DECIMAL))
        self.write(struct.pack("<q", integer))
        self.write(struct.pack("<Q", fractional))

    def write_array(self, elements):
        self.write(struct.pack("B", TYPE_ARRAY))
        self.write(struct.pack("<I", len(elements)))
        for elem in elements:
            self.write_any(self._auto_type(elem))

    def write_object(self, obj: dict):
        self.write(struct.pack("B", TYPE_OBJECT))
        self.write(struct.pack("<I", len(obj)))
        for k, v in obj.items():
            self.write_any((TYPE_STRING, str(k)))
            self.write_any(self._auto_type(v))

    def _auto_type(self, v):
        if v is None:
            return (TYPE_NULL, None)
        elif isinstance(v, bool):
            return (TYPE_BOOL, v)
        elif isinstance(v, int):
            return (TYPE_UINT, v) if v >= 0 else (TYPE_INT, v)
        elif isinstance(v, float):
            return (TYPE_FLOAT, v)
        elif isinstance(v, str):
            return (TYPE_STRING, v)
        elif isinstance(v, bytes):
            return (TYPE_BLOB, v)
        elif isinstance(v, (list, tuple)):
            return (TYPE_ARRAY, list(v))
        elif isinstance(v, dict):
            return (TYPE_OBJECT, v)
        else:
            raise TypeError(f"Unsupported type: {type(v)}")

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
            self.write_decimal(*v)
        elif t == TYPE_ARRAY:
            self.write_array(v)
        elif t == TYPE_OBJECT:
            self.write_object(v)

    def build(self, root):
        self.write(b"ck")
        body = CakeWriter()
        body.write_any(body._auto_type(root))
        self.write(struct.pack("<Q", len(body.buf)))
        self.write(body.buf)
        return self.buf

# ==============================
# Reader
# ==============================
class CakeReader:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def read(self, size):
        res = self.data[self.pos:self.pos+size]
        self.pos += size
        return res

    def read_null(self):
        return None

    def read_bool(self):
        return self.read(1)[0] == 1

    def read_int(self):
        return struct.unpack("<q", self.read(8))[0]

    def read_uint(self):
        return struct.unpack("<Q", self.read(8))[0]

    def read_float(self):
        return struct.unpack("<d", self.read(8))[0]

    def read_string(self):
        length = struct.unpack("<I", self.read(4))[0]
        return self.read(length).decode("utf-8")

    def read_blob(self):
        length = struct.unpack("<I", self.read(4))[0]
        return self.read(length)

    def read_date(self):
        return struct.unpack("<Q", self.read(8))[0]

    def read_decimal(self):
        return (struct.unpack("<q", self.read(8))[0], struct.unpack("<Q", self.read(8))[0])

    def read_array(self):
        length = struct.unpack("<I", self.read(4))[0]
        return [self.read_any() for _ in range(length)]

    def read_object(self):
        length = struct.unpack("<I", self.read(4))[0]
        obj = {}
        for _ in range(length):
            k = self.read_any()
            v = self.read_any()
            obj[k] = v
        return obj

    def read_any(self):
        t = self.read(1)[0]
        if t == TYPE_NULL:
            return self.read_null()
        elif t == TYPE_BOOL:
            return self.read_bool()
        elif t == TYPE_INT:
            return self.read_int()
        elif t == TYPE_UINT:
            return self.read_uint()
        elif t == TYPE_FLOAT:
            return self.read_float()
        elif t == TYPE_STRING:
            return self.read_string()
        elif t == TYPE_BLOB:
            return self.read_blob()
        elif t == TYPE_DATE:
            return self.read_date()
        elif t == TYPE_DECIMAL:
            return self.read_decimal()
        elif t == TYPE_ARRAY:
            return self.read_array()
        elif t == TYPE_OBJECT:
            return self.read_object()
        else:
            raise ValueError(f"Unknown type: {t}")

    def parse(self):
        if self.read(2) != b"ck":
            raise ValueError("Invalid ck file")
        self.read(8)
        return self.read_any()

# ==============================
# autoexp
# ==============================
def autoexp(path=None):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(db, *args, **kwargs):
            try:
                res = func(db, *args, **kwargs)
                success = ("insert success", "update success", "delete success", "clear success", "imp success", "j2d success")
                if isinstance(res, str) and any(res.startswith(s) for s in success):
                    if path is None:
                        db.exp()
                    else:
                        db.exp(path)
                return res
            except Exception as e:
                return f"autoexp error: {e}"
        return wrapper
    return decorator

# ==============================
# dbtypelock
# ==============================
def dbtypelock(mode="all"):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(db, *args, **kwargs):
            try:
                if not hasattr(db, "data") or db.data is None:
                    return "dbtypelock error: not imported"
                fname = func.__name__
                if len(args) < 1:
                    return func(db, *args, **kwargs)
                key = args[0]
                val = args[1] if len(args) > 1 else None

                if fname in ("insert", "update") and key in db.data:
                    old_val = db.data[key]
                    lval = mode in ("all", "value")
                    lkey = mode in ("all", "key")

                    if lval and val is not None:
                        pass
                    if lkey:
                        pass
            except Exception:
                pass
            return func(db, *args, **kwargs)
        return wrapper
    if callable(mode):
        return decorator(mode)
    return decorator

# ==============================
# MAIN DB CLASS
# ==============================
class CakeDB:
    def __init__(self):
        self.current_file = None
        self.data = None

    def imp(self, path):
        try:
            with open(path, "rb") as f:
                data = f.read()
            self.data = CakeReader(data).parse()
            self.current_file = path
            return f"imp success: {path}"
        except Exception as e:
            return f"imp error: {e}"

    def exp(self, path=None):
        try:
            if self.data is None:
                return "exp error: no data"
            p = path or self.current_file
            buf = CakeWriter().build(self.data)
            with open(p, "wb") as f:
                f.write(buf)
            return f"exp success: {p}"
        except Exception as e:
            return f"exp error: {e}"

    def j2d(self, jpath, out=None):
        try:
            with open(jpath, "r", encoding="utf-8") as f:
                obj = json.load(f)
            self.data = obj
            self.current_file = out or os.path.splitext(jpath)[0] + ".ck"
            return self.exp()
        except Exception as e:
            return f"j2d error: {e}"

    def d2j(self, out=None):
        try:
            if self.data is None:
                return "d2j error: no data"
            p = out or os.path.splitext(self.current_file)[0] + ".json"
            def safe(o):
                if isinstance(o, bytes):
                    return base64.b64encode(o).decode()
                if isinstance(o, (list, tuple)):
                    return [safe(i) for i in o]
                if isinstance(o, dict):
                    return {str(k): safe(v) for k, v in o.items()}
                return o
            with open(p, "w", encoding="utf-8") as f:
                json.dump(safe(self.data), f, ensure_ascii=False, indent=2)
            return f"d2j success: {p}"
        except Exception as e:
            return f"d2j error: {e}"

    def insert(self, key, val):
        try:
            if self.data is None:
                self.data = {}
            self.data[key] = val
            return f"insert success: {key}"
        except Exception as e:
            return f"insert error: {e}"

    def update(self, key, val):
        return self.insert(key, val)

    def delete(self, key):
        try:
            del self.data[key]
            return f"delete success: {key}"
        except Exception as e:
            return f"delete error: {e}"

    def clear(self):
        try:
            self.data = {}
            return "clear success"
        except Exception as e:
            return f"clear error: {e}"

    def select(self, key=None):
        try:
            if key is None:
                return f"select success: {json.dumps(self.data, ensure_ascii=False)}"
            return f"select success: {json.dumps(self.data.get(key), ensure_ascii=False)}"
        except Exception as e:
            return f"select error: {e}"

    def count(self):
        try:
            return f"count success: {len(self.data) if isinstance(self.data, (dict, list)) else 0}"
        except Exception as e:
            return f"count error: {e}"
