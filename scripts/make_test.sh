#!/bin/bash

# 检查输入参数
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 projectname test_count thread_count"
    exit 1
fi

projectname=$1
test_count=$2
thread_count=$3
output_file="test_output.txt"
time_file="time_output.txt"
output_dir="test_output"

# 创建输出目录
mkdir -p "$output_dir"

# 定义单个测试函数
run_test() {
    local test_num=$1
    local projectname=$2
    local output_dir=$3

    echo "Running test $test_num for project $projectname..."
    start_time=$(date +%s.%N)
    output=$(make $projectname 2>&1)
    end_time=$(date +%s.%N)
    duration=$(echo "$end_time - $start_time" | bc)

    if echo "$output" | grep -qiE "fail|error|fatal"; then
        echo "Test $test_num failed."
        echo "failed" >> "$output_file"
        echo "$output" > "${output_dir}/output_${projectname}_${test_num}.log"
    else
        echo "Test $test_num succeeded."
        echo "succeeded" >> "$output_file"
    fi

    echo "$duration" >> "$time_file"
}

cd ..
export -f run_test
export output_file
export time_file
export output_dir

# 清空输出文件
true > "$output_file"
true > "$time_file"

# 使用 GNU Parallel 进行并行测试，并禁用引用消息
seq 1 "$test_count" | parallel --no-notice -j "$thread_count" run_test {} "$projectname" "$output_dir"

# 统计结果
success_count=$(grep -c "succeeded" "$output_file")
fail_count=$(grep -c "failed" "$output_file")

# 计算成功比例
success_rate=$(echo "scale=2; $success_count / $test_count * 100" | bc)

# 计算平均时间
total_time=$(awk '{s+=$1} END {print s}' "$time_file")
average_time=$(echo "scale=2; $total_time / $test_count" | bc)

# 输出结果
echo "===================="
echo "Test results for project $projectname:"
echo "Total tests: $test_count"
echo "Successful tests: $success_count"
echo "Failed tests: $fail_count"
echo "Success rate: $success_rate%"
echo "Total time: $total_time seconds"
echo "Average time per test: $average_time seconds"

# 删除临时文件
rm -f "$output_file"
rm -f "$time_file"
