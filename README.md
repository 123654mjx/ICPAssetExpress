# ICPAssetExpress

<font style="color:rgb(31, 35, 40);">郑重声明：文中所涉及的技术、思路和工具仅供以安全为目的的学习交流使用，任何人不得将其用于非法用途以及盈利等目的，否则后果自行承担。</font>

# <font style="color:rgb(31, 35, 40);">0x01 简介</font>
<font style="color:rgb(31, 35, 40);">一款互联网有效资产发现工具，方便快速对大量企业进行信息收集，快速打点</font>

<font style="color:rgb(31, 35, 40);">（目前基于360 quake会员api）</font>

# <font style="color:rgb(31, 35, 40);">0x02 功能</font>
1. 调用 <font style="color:rgb(31, 35, 40);">360 quake api对目标企业关键词（icp_keywords）进行查询</font>
2. <font style="color:rgb(31, 35, 40);">调用 fscan1.8.2 以及 observer_ward 对接口查询到的 ip 及 url 进行端口探测及指纹识别</font>
3. <font style="color:rgb(31, 35, 40);">调用fofa api 对接口查询到的 ip 进行反查（注意：已内置部分过滤cdn、邮箱等公共服务规则，但覆盖面仍不全面，后续将持续优化）</font>
4. <font style="color:rgb(31, 35, 40);">调用实时查询icp接口（</font>[https://api2.wer.plus/](https://api2.wer.plus/)<font style="color:rgb(31, 35, 40);">）获取工信部备案目标企业所属小程序、app信息</font>
5. <font style="color:rgb(31, 35, 40);">在对应企业文件夹写入结果</font>

```python
支持两种运行模式：

1. 基础模式 (-b, --basic):
   - Quake API 查询获取资产 (IP, URL等)
   - 将提取的IP和URL分别保存到 .txt 文件
   - 对从Quake获取的URL进行指纹识别 (observer_ward)
   - 结果整理输出 (Excel, .txt文件归档)

2. 高级模式 (-a, --advanced, 默认):
   - Quake API 查询获取资产
   - 对提取的IP进行fscan扫描
   - 对从Quake获取的URL进行初次指纹识别
   - 对fscan发现的新URL进行二次指纹识别
   - 结果整理输出 (Excel, .txt文件归档)

可配置项见后续具体使用部分
```

<img src="https://cdn.nlark.com/yuque/0/2025/png/39031852/1751248440805-9bead298-48b8-4083-838f-a42eaa85f3ca.png" style="zoom:67%;" />

# <font style="color:rgb(31, 35, 40);">0x03 使用</font>

1. **按照observer_ward项目中所示运行observer_ward或手动更新指纹（不然无法进行指纹识别）**

项目地址： [https://github.com/emo-crab/observer_ward](https://github.com/emo-crab/observer_ward)

2. **安装所需依赖**

```plain
pip install -r requirements.txt 
```

3. **<font style="color:rgb(31, 35, 40);">各平台 api_key 、默认端口、基础语句模板、缓存有效期等参数可自行设置调整</font>**

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751211625442-744ad3cb-97ed-4910-af2c-6182df81f73e.png)

【注意】

<font style="color:rgb(31, 35, 40);">目前查询工信部备案数据（小程序、app）功能需于 </font>[https://api2.wer.plus/doc/14](https://api2.wer.plus/doc/14) <font style="color:rgb(31, 35, 40);">购买api接口获取Key</font>

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751212401234-46f1c1ca-2ea9-4d45-8f7c-420274bb32fc.png)

4. **在 icpCheck.txt 文件中按行写入目标单位关键词**

【注意】

+ 关键词会进行模糊匹配，如查询xx集团，则会查询出xx集团a公司、xx集团b公司...
+ <font style="color:rgb(31, 35, 40);">扫描 ip 端口为默认端口+目标公司查询结果端口去重</font>
+ 为避免终端输出信息过多，调用工具默认<font style="color:rgb(31, 35, 40);">静默模式，如需可自行添加 --showScanInfo 参数</font>
+ 为避免垃圾数据和积分浪费，脚本中查询语句如下，可自行在配置处修改

PS：如果需要模糊匹配使更精确，可添加条件如 icp:"京icp"

```plain
icp_keywords:"{target}" and not domain_is_wildcard:true and country:"China" AND not province:"Hongkong"
```

5. **<font style="color:rgb(31, 35, 40);">运行命令，可见详情</font>**

```plain
python ICPAssetExpress.py -h
```

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751211885603-16ba289e-ea22-4ba5-96b3-bdaf7771f35f.png)

```plain
# 建议开启 --skip-fofa-fingerprint（跳过对fofa反查ip获取url的指纹识别）
（ip反查存在共享服务未过滤完全情况，可能产生大量垃圾数据，导致指纹识别大幅降低效率）

【命令示例】
# 基础模式（仅扫描quake url），查询备案小程序及app，fofa反查ip（跳过fofa url识别）
python ICPAssetExpress 5.0.py -b -checkother "app,mapp" --skip-fofa-fingerprint -o 输出目录

# 高级模式（扫描quake url，调用fscan扫描ip），查询备案小程序，跳过fofa反查ip
python ICPAssetExpress 5.0.py -a -checkother "mapp" --no-fofa -o 输出目录

# merge脚本（遍历输出结果合并quake、指纹识别、fscan结果、ip反查结果）
python merge.py -t 存放结果目录 -o 输出目录

【注意】
若未配置相关接口，不使用相关模块即可，具体如下：
● 未配置fofa key，可使用 --nofofa 跳过fofa调用阶段
● 未配置第三方工信部备案数据查询接口 key，不添加 -checkother 即可跳过该功能
```

6. **以公司为单位输出结果，包含全部探测结果，具体内容自行查看**

【注意】

+ 需等脚本全部运行完毕才能正常获取结果
+ 过程中处理的 txt 文件存放在related materials文件夹中，对工具处理方式不满意可二次自行处理
+ 运行结束后会生成自查结果以及日志文件 log.txt，出现报错及查询失败可自行排查，
+ 通过自查结果可快速判断当前缓存所有资产

# <font style="color:rgb(31, 35, 40);">0x04 效果截图</font>
1. 输出目录

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751218745414-a5f27856-d5b1-4e50-9dd0-6658f419a3ce.png)

2. 小程序、app查询输出结果

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751218618812-2ec27e6c-e390-442f-b0ff-916b612d50ab.png)

3. 自查报告输出结果

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751216925325-a1f92fc4-b086-4730-a219-25823d07fa4c.png)![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751246597256-f9a6b26e-bf09-45ed-bdbb-6ccac0fdf36c.png)

4. 具体目标单位处理结果（分为各个阶段处理结果）

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751246401269-ae8af290-a50c-46d8-ba3f-60b0f8a95d14.png)

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751249024726-4cffd017-6b76-4495-bf98-ce3211b2ede3.png)

5. merge脚本合并多个单位结果（部分效果）

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751245873201-f74e4e20-4377-4fb5-8b6a-1effbd3a3e37.png)

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1751246249588-5f1ae56f-3f71-477c-bf0d-7dbdf5f26af7.png)

# <font style="color:rgb(31, 35, 40);">0x05 致谢</font>
本脚本是为提高工作效率用 AI 完成的缝合工具，感谢各位师傅的开源项目和提议！！

<font style="color:rgb(31, 35, 40);">gh0stkey</font>

<font style="color:rgb(31, 35, 40);">樱花庄的本间白猫</font>

[https://github.com/emo-crab/observer_ward](https://github.com/emo-crab/observer_ward)

[https://github.com/shadow1ng/fscan](https://github.com/shadow1ng/fscan)

[https://github.com/yz1639/FscanOutputBeautify](https://github.com/yz1639/FscanOutputBeautify)

# <font style="color:rgb(31, 35, 40);">0x06 补充</font>
**2025.5.19	**

**添加结果合并脚本（merge.py）**

+ **应用场景：**同一集团下大量相关企业，结果按公司输出至文件较为繁琐，且可能大部分公司发现资产很少
+ **用途：**合并指定目录下所有quake资产、指纹识别、端口扫描结果，同时标注数据来源表格

![](https://cdn.nlark.com/yuque/0/2025/png/39031852/1747646993640-19ac82c1-afd9-4187-b1ab-f789ccfc9bd9.png)

**2025.6.29	**

1. **添加 fofa（ip反查）及其他备案资产（app、小程序）查询功能**
2. **添加全部查询结果的缓存机制**
3. **添加自查机制，运行完毕输出本次运行状态及缓存所有资产便于自行梳理**
4. **添加结果合并脚本（merge.py）的fofa结果合并功能**

****



