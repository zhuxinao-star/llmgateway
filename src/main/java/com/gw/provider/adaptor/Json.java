package com.gw.provider.adaptor;

import java.util.*;

/**
 * 无依赖的极简 JSON 工具：够用的解析 + 序列化。
 * 专为网关场景：解析上游请求/响应、构造转发 JSON。
 * （Jackson 已随 spring-boot-starter-web 引入，此处为了核心转换逻辑零依赖、易单测而实现；
 *   生产路径实际共用本类的纯函数语义。）
 */
public final class Json {

    private Json() {}

    // ================= 解析 =================

    public static Object parse(String json) {
        Parser p = new Parser(json);
        Object v = p.parseValue();
        p.skipWs();
        if (!p.eof()) throw new IllegalArgumentException("JSON 尾部有多余字符: " + json.substring(p.pos));
        return v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String json) {
        Object v = parse(json);
        if (!(v instanceof Map)) throw new IllegalArgumentException("期望 JSON 对象");
        return (Map<String, Object>) v;
    }

    public static Map<String, Object> obj(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    public static List<Object> arr(Object... items) {
        return new ArrayList<>(Arrays.asList(items));
    }

    // ================= 序列化 =================

    public static String write(Object v) {
        StringBuilder sb = new StringBuilder(256);
        writeValue(v, sb);
        return sb.toString();
    }

    private static void writeValue(Object v, StringBuilder sb) {
        if (v == null) { sb.append("null"); return; }
        if (v instanceof String s) { writeString(s, sb); return; }
        if (v instanceof Boolean b) { sb.append(b); return; }
        if (v instanceof Double d) {
            if (d.isNaN() || d.isInfinite()) sb.append("null");
            else if (d == Math.floor(d) && !d.isInfinite() && Math.abs(d) < 1e15) sb.append((long) (double) d);
            else sb.append(d);
            return;
        }
        if (v instanceof Number n) { sb.append(n); return; }
        if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                writeString(String.valueOf(e.getKey()), sb);
                sb.append(':');
                writeValue(e.getValue(), sb);
            }
            sb.append('}');
            return;
        }
        if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) sb.append(',');
                first = false;
                writeValue(o, sb);
            }
            sb.append(']');
            return;
        }
        writeString(v.toString(), sb);
    }

    private static void writeString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    // ================= Parser =================

    private static final class Parser {
        final String s;
        int pos = 0;

        Parser(String s) { this.s = s; }

        boolean eof() { return pos >= s.length(); }

        void skipWs() {
            while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
        }

        char peek() {
            if (eof()) throw new IllegalArgumentException("JSON 意外结束");
            return s.charAt(pos);
        }

        Object parseValue() {
            skipWs();
            char c = peek();
            return switch (c) {
                case '{' -> parseObj();
                case '[' -> parseArr();
                case '"' -> parseStr();
                case 't' -> { expect("true"); yield Boolean.TRUE; }
                case 'f' -> { expect("false"); yield Boolean.FALSE; }
                case 'n' -> { expect("null"); yield null; }
                default -> parseNum();
            };
        }

        void expect(String word) {
            if (!s.startsWith(word, pos)) throw new IllegalArgumentException("JSON 非法: " + s.substring(Math.max(0, pos - 2), Math.min(s.length(), pos + 8)));
            pos += word.length();
        }

        Map<String, Object> parseObj() {
            Map<String, Object> m = new LinkedHashMap<>();
            pos++; // {
            skipWs();
            if (peek() == '}') { pos++; return m; }
            while (true) {
                skipWs();
                if (peek() != '"') throw new IllegalArgumentException("对象键必须是字符串");
                String k = parseStr();
                skipWs();
                if (peek() != ':') throw new IllegalArgumentException("期望 ':'");
                pos++;
                m.put(k, parseValue());
                skipWs();
                char c = peek();
                if (c == ',') { pos++; continue; }
                if (c == '}') { pos++; return m; }
                throw new IllegalArgumentException("对象格式错误");
            }
        }

        List<Object> parseArr() {
            List<Object> l = new ArrayList<>();
            pos++; // [
            skipWs();
            if (peek() == ']') { pos++; return l; }
            while (true) {
                l.add(parseValue());
                skipWs();
                char c = peek();
                if (c == ',') { pos++; continue; }
                if (c == ']') { pos++; return l; }
                throw new IllegalArgumentException("数组格式错误");
            }
        }

        String parseStr() {
            pos++; // "
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (eof()) throw new IllegalArgumentException("字符串未闭合");
                char c = s.charAt(pos++);
                if (c == '"') return sb.toString();
                if (c == '\\') {
                    if (eof()) throw new IllegalArgumentException("转义未闭合");
                    char e = s.charAt(pos++);
                    switch (e) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'u' -> {
                            if (pos + 4 > s.length()) throw new IllegalArgumentException("\\u 转义不完整");
                            sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                            pos += 4;
                        }
                        default -> throw new IllegalArgumentException("非法转义 \\" + e);
                    }
                } else {
                    sb.append(c);
                }
            }
        }

        Object parseNum() {
            int start = pos;
            while (pos < s.length()) {
                char c = s.charAt(pos);
                if (Character.isDigit(c) || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') pos++;
                else break;
            }
            String num = s.substring(start, pos);
            if (num.isEmpty()) throw new IllegalArgumentException("数字格式错误");
            if (num.indexOf('.') < 0 && num.indexOf('e') < 0 && num.indexOf('E') < 0) {
                try { return Long.parseLong(num); } catch (NumberFormatException e) { return Double.parseDouble(num); }
            }
            return Double.parseDouble(num);
        }
    }

    // ================= 便捷取值 =================

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asMap(Object v) {
        return v instanceof Map ? (Map<String, Object>) v : null;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> asList(Object v) {
        return v instanceof List ? (List<Object>) v : null;
    }

    public static String str(Object v) {
        return v == null ? null : String.valueOf(v);
    }

    public static Long longVal(Object v) {
        if (v == null) return null;
        if (v instanceof Number n) return n.longValue();
        try { return Long.parseLong(String.valueOf(v)); } catch (NumberFormatException e) { return null; }
    }

    public static Integer intVal(Object v) {
        Long l = longVal(v);
        return l == null ? null : l.intValue();
    }

    public static boolean boolVal(Object v, boolean dflt) {
        if (v instanceof Boolean b) return b;
        if (v instanceof String s) return Boolean.parseBoolean(s);
        return dflt;
    }
}
