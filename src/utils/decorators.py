# src/utils/output_control.py

import sys
import os
import functools
from datetime import datetime
import inspect


def output_controller(func):
    """
    一个装饰器，用于控制函数内print语句的输出行为。

    根据被装饰函数的是否传入 echo=True 参数，决定输出到控制台还是日志文件。
    - echo=True: 在控制台输出，并在内容前添加一行来源函数信息。
    - echo=False: 将所有输出重定向到 a_project_root/logs/ 文件夹下的日志文件中。
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # 使用 inspect 模块安全地获取 echo 参数的值，无论它是位置参数还是关键字参数
        # 如果函数签名中没有 echo 参数或调用时未提供，则默认为 False
        try:
            bound_args = inspect.signature(func).bind(*args, **kwargs)
            bound_args.apply_defaults()
            echo = bound_args.arguments.get('echo', False)
        except TypeError:
            # 如果绑定失败（例如，调用时的参数不匹配函数签名），则默认不输出到控制台
            echo = False

        # 准备要添加的头部消息
        header_message = f"【来自{func.__name__}函数的消息:】"

        if echo:
            # echo=True，输出到控制台
            print(header_message)
            # 执行原函数，其内部的print会正常输出到控制台
            result = func(*args, **kwargs)
            return result
        else:
            # echo=False，重定向输出到日志文件

            # 1. 确定日志文件路径
            # 获取当前文件（output_control.py）的绝对路径
            current_file_path = os.path.abspath(__file__)
            # 从 src/utils/output_control.py 推断出项目根目录
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file_path)))
            log_dir = os.path.join(project_root, 'logs')

            # 确保 logs 文件夹存在
            os.makedirs(log_dir, exist_ok=True)

            # 2. 创建日志文件名 (YYYYMMDD-HHMMSS.log)
            timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            log_filename = f"{timestamp}.log"
            log_filepath = os.path.join(log_dir, log_filename)

            # 3. 重定向标准输出
            original_stdout = sys.stdout  # 保存原始的标准输出
            try:
                with open(log_filepath, 'w', encoding='utf-8') as log_file:
                    sys.stdout = log_file  # 将标准输出重定向到日志文件
                    print(header_message)  # 在日志文件顶部写入头部消息
                    result = func(*args, **kwargs)  # 执行原函数，其print会写入文件
                return result
            finally:
                # 无论函数是否出错，都必须恢复标准输出
                sys.stdout = original_stdout

    return wrapper


# --- 使用示例 ---

@output_controller
def get_test(category_name, stock_list, echo=True):
    """
    一个模拟获取某行业分类股票的函数。
    """
    print(f"表格 'sw_category' 已存在，无需创建")
    # 为了演示，我们只截取前5个股票
    display_stocks = stock_list[:5]
    print(f"{category_name}: {display_stocks}等股票")
    return len(stock_list)


# --- 主程序 ---
if __name__ == "__main__":
    stocks = ['000021.SZ', '000050.SZ', '000413.SZ', '600707.SH', '600601.SH', '300750.SZ']

    print("--- 场景1: echo=True (默认行为，输出到控制台) ---")
    total_stocks = get_test("电子信息类", stocks, echo=True)
    print(f"函数返回: 共处理了 {total_stocks} 只股票。\n")

    print("--- 场景2: echo=False (输出到日志文件) ---")
    total_stocks_log = get_test("电子信息类", stocks, echo=False)
    print(f"函数返回: 共处理了 {total_stocks_log} 只股票。\n")

    print("请检查项目文件夹下的 'logs' 目录，查看生成的日志文件。")

#def show_func_name(func):
#    @functools.wraps(func)
#    def wrapper(*args, **kwargs):
#        # 获取echo参数的位置
#        sig = inspect.signature(func)
#        params = sig.parameters
#        param_names = list(params.keys())
#        echo = None
#        # 先找关键字参数
#        if 'echo' in kwargs:
#            echo = kwargs['echo']
#        else:
#            # 再找位置参数
#            if 'echo' in param_names:
#                index = param_names.index('echo')
#                if index < len(args):
#                    echo = args[index]
#        # echo默认值处理
#        if echo is None and 'echo' in params:
#            echo = params['echo'].default
#        # 判断是否需要输出
#        if echo:
#            print(f"【来自{func.__name__}函数的消息:】")
#            return func(*args, **kwargs)
#        # echo为False时直接返回，不输出
#        return
#    return wrapper