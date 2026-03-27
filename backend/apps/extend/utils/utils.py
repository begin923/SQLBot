import os
from typing import Dict

import yaml


class Utils:
    """工具类"""

    @staticmethod
    def load_prompt_template_static(template_name: str) -> Dict[str, str]:
        """静态方法：加载提示词模板（可被 ModelClient 复用）"""
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        yaml_path = os.path.join(current_dir, "yaml")
        yaml_file = os.path.join(yaml_path, f"{template_name}.yaml")
        print(yaml_file)

        if os.path.exists(yaml_file):
            with open(yaml_file, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        else:
            raise FileNotFoundError(f"提示词文件不存在：{yaml_file}")


if __name__ == "__main__":
    print(Utils.load_prompt_template_static("rule_keyword"))