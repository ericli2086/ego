#!/bin/bash

# 以HTTP响应状态码为判据，并总是打印响应内容的RESTful API测试+统计报告
API="http://localhost:8080/api/rest"
DB="test"
TABLE="user"
KEY="id"
TMP=resp.json
pass=0
fail=0
total=0

# 统计单用例函数
run_case() {
  ((total++))
  local desc="$1"
  local cmd="$2"
  local expect_code="$3"
  local check="$4"
  local expect_value="$5"

  echo -e "\n[$total] $desc"
  # -w '%{http_code}' 输出HTTP状态码到stdout，-o $TMP 将响应体输出到文件
  http_code=$(eval "$cmd -o $TMP -w '%{http_code}'")

  echo "HTTP状态码: $http_code"
  echo "响应内容:"
  cat $TMP
  echo

  # 判断状态码
  if [[ "$http_code" == "$expect_code" ]]; then
    if [[ -n "$check" ]]; then
      res=$(jq -r "$check" $TMP 2>/dev/null)
      if [[ "$res" == "$expect_value" ]]; then
        echo "✅ 用例通过"
        ((pass++))
      else
        echo "❌ 状态码对，内容错: Got '$res', Expect '$expect_value'"
        ((fail++))
      fi
    else
      echo "✅ 用例通过"
      ((pass++))
    fi
  else
    echo "❌ 状态码错误 (Got: $http_code, Expect: $expect_code)"
    ((fail++))
  fi
}

# 清理
cleanup() { rm -f $TMP; }
trap cleanup EXIT

echo "======== RESTful API 自动化测试(状态码判据+响应内容打印) ========"

TIME=$(date +%s%3N)
# 1. 批量创建
run_case "批量创建" \
  "curl -s -X POST \"$API/$DB/$TABLE\" -H \"Content-Type: application/json\" -d '[
    {\"username\":\"alice$TIME\",\"email\":\"alice$TIME@example.com\",\"age\":1},
    {\"username\":\"alice2$TIME\",\"email\":\"alice2$TIME@example.com\",\"age\":2},
    {\"username\":\"alice3$TIME\",\"email\":\"alice3$TIME@example.com\",\"age\":3},
    {\"username\":\"alice4$TIME\",\"email\":\"alice4$TIME@example.com\",\"age\":4},
    {\"username\":\"alice5$TIME\",\"email\":\"alice5$TIME@example.com\",\"age\":5},
    {\"username\":\"alice6$TIME\",\"email\":\"alice6$TIME@example.com\",\"age\":6},
    {\"username\":\"alice7$TIME\",\"email\":\"alice7$TIME@example.com\",\"age\":7},
    {\"username\":\"alice8$TIME\",\"email\":\"alice8$TIME@example.com\",\"age\":8},
    {\"username\":\"alice9$TIME\",\"email\":\"alice9$TIME@example.com\",\"age\":9},
    {\"username\":\"alice10$TIME\",\"email\":\"alice10$TIME@example.com\",\"age\":10},
    {\"username\":\"alice11$TIME\",\"email\":\"alice11$TIME@example.com\",\"age\":11},
    {\"username\":\"alice12$TIME\",\"email\":\"alice12$TIME@example.com\",\"age\":12},
    {\"username\":\"alice13$TIME\",\"email\":\"alice13$TIME@example.com\",\"age\":13},
    {\"username\":\"bob$TIME\",\"email\":\"bob$TIME@example.com\",\"age\":14}
  ]'" \
  "201"

# 2. 查询全部
run_case "查询全部" \
  "curl -s \"$API/$DB/$TABLE?page=1&page_size=2\"" \
  "200" \
  ".data | length == 2" "true"

# 3. username过滤
run_case "username=alice$TIME" \
  "curl -s \"$API/$DB/$TABLE?username=alice$TIME\"" \
  "200" \
  ".data[0].username" "alice$TIME"

# 4. email过滤
run_case "email=bob$TIME@example.com" \
  "curl -s \"$API/$DB/$TABLE?email=bob$TIME@example.com\"" \
  "200" \
  ".data[0].email" "bob$TIME@example.com"

# 5. 唯一键 email 单查
run_case "单查 email=bob$TIME@example.com" \
  "curl -s \"$API/$DB/$TABLE/bob$TIME@example.com?key=email\"" \
  "200" \
  ".email" "bob$TIME@example.com"

# 7. 主键单查
ID_ALICE=$(curl -s "$API/$DB/$TABLE?username=alice$TIME" | jq -r ".data[0].$KEY")
ID_BOB=$(curl -s "$API/$DB/$TABLE?username=bob$TIME" | jq -r ".data[0].$KEY")
run_case "主键查 alice$TIME" \
  "curl -s \"$API/$DB/$TABLE/$ID_ALICE\"" \
  "200" \
  ".username" "alice$TIME"

run_case "主键查 bob$TIME" \
  "curl -s \"$API/$DB/$TABLE/$ID_BOB\"" \
  "200" \
  ".username" "bob$TIME"

# 8. 分页
run_case "分页" \
  "curl -s \"$API/$DB/$TABLE?page=1&page_size=1\"" \
  "200" \
  ".data | length" "1"

# 9. 字段选择
run_case "字段选择 username" \
  "curl -s \"$API/$DB/$TABLE/$ID_ALICE?fields=username\"" \
  "200" \
  ".username" "alice$TIME"

# 10. 排序
run_case "按 username 倒序" \
  "curl -s \"$API/$DB/$TABLE?order=-$KEY&fields=username\"" \
  "200" \
  ".data[0].username" "bob$TIME"

# 11. like 查询
run_case "username__like=ali" \
  "curl -s \"$API/$DB/$TABLE?username__like=ali%25&fields=username\"" \
  "200" \
  ".data | length > 0" "true"

# 12. in 查询
run_case "username__in=alice$TIME,bob$TIME" \
  "curl -s \"$API/$DB/$TABLE?username__in=alice$TIME,bob$TIME\"" \
  "200" \
  ".data | length" "2"

# 13. 批量更新
CTIME=$(date +%s%3N)
run_case "批量更新 alice$TIME/bob$TIME 邮箱" \
  "curl -s -X PUT \"$API/$DB/$TABLE\" -H \"Content-Type: application/json\" -d '[
    {\"$KEY\":\"$ID_ALICE\",\"email\":\"alice$CTIME@example.com\"},
    {\"$KEY\":\"$ID_BOB\",\"email\":\"bob$CTIME@example.com\"}
  ]'" \
  "200" \
  ".matched_count" "2"

# 14. 单条更新
run_case "单条更新 alice username" \
  "curl -s -X PUT \"$API/$DB/$TABLE/$ID_ALICE\" -H \"Content-Type: application/json\" -d '{\"username\":\"alice_new\"}'" \
  "200" \
  ".matched_count" "1"

# 15. 单条删除
run_case "单条删除 bob" \
  "curl -s -X DELETE \"$API/$DB/$TABLE/$ID_BOB\"" \
  "200" \
  ".deleted_count" "1"

# 16. 批量删除
run_case "批量删除 alice_new $ID_ALICE" \
  "curl -s -X POST \"$API/$DB/$TABLE/batch_delete\" -H \"Content-Type: application/json\" -d '[\"$ID_ALICE\"]'" \
  "200" \
  ".deleted_count" "1"

# 17. 查询不存在
run_case "查询不存在 email=not_exist" \
  "curl -s \"$API/$DB/$TABLE/not_exist?key=email\"" \
  "404"

# 18. 非法批量更新（缺主键）
run_case "非法批量更新缺主键" \
  "curl -s -X PUT \"$API/$DB/$TABLE\" -H \"Content-Type: application/json\" -d '[{\"email\":\"x@x.com\"}]'" \
  "400"

# 19. 非法批量删除（空数组）
run_case "非法批量删除空数组" \
  "curl -s -X POST \"$API/$DB/$TABLE/batch_delete\" -H \"Content-Type: application/json\" -d '[]'" \
  "400"

# 20. icontains 查询
run_case "username__icontains=user_" \
  "curl -s \"$API/$DB/$TABLE?username__icontains=user_&page=1&page_size=10&fields=$KEY,username\"" \
  "200" \
  ".data | length > 0" "true"

# 21. isnull 查询
run_case "username__isnull=true" \
  "curl -s \"$API/$DB/$TABLE?username__isnull=true&page=1&page_size=10&fields=$KEY,username\"" \
  "200" \
  ".data | length == 0" "true"

KEY="age"
# 22. gte 查询
run_case "${KEY}__gte=4" \
  "curl -s \"$API/$DB/$TABLE?${KEY}__gte=4&page=1&page_size=10&fields=$KEY,username\"" \
  "200" \
  ".data[0].$KEY" "4"

# 23. lte 查询
run_case "${KEY}__lte=10" \
  "curl -s \"$API/$DB/$TABLE?${KEY}__lte=10&page=1&page_size=10&fields=$KEY,username&order=-$KEY\"" \
  "200" \
  ".data[0].$KEY" "10"

# 24. gt 查询
run_case "${KEY}__gt=10" \
  "curl -s \"$API/$DB/$TABLE?${KEY}__gt=10&page=1&page_size=10&fields=$KEY,username\"" \
  "200" \
  ".data[0].$KEY" "11"

# 25. lt 查询
run_case "${KEY}__lt=10" \
  "curl -s \"$API/$DB/$TABLE?${KEY}__lt=10&page=1&page_size=10&fields=$KEY,username&order=-$KEY\"" \
  "200" \
  ".data[0].$KEY" "9"

# 26. ne 查询
run_case "${KEY}__ne=9" \
  "curl -s \"$API/$DB/$TABLE?${KEY}__ne=9&page=1&page_size=10&fields=$KEY,username\"" \
  "200" \
  ".data | length > 0" "true"

# 27. between 查询
run_case "${KEY}__between=2,10" \
  "curl -s \"$API/$DB/$TABLE?${KEY}__between=2,10&fields=$KEY,username\"" \
  "200" \
  ".data | length > 0" "true"


echo
echo "======== 测试报告 ========"
echo "总计: $total"
echo "通过: $pass"
echo "失败: $fail"
if [[ $fail -eq 0 ]]; then
  echo "🎉 全部接口通过测试！"
else
  echo "❌ 有失败用例，请检查上方详情！"
fi