## 使用说明

目前添加了8个函数

### chineseFulltextIndex.removeIndex
删除数据库中的所有索引

使用：call chineseFulltextIndex.removeIndex

### chineseFulltextIndex.removeIndexByLabel(String labelName)
根据标签名删除索引

使用：call chineseFulltextIndex.removeIndexByLabel("labelName")

### chineseFulltextIndex.addNodeIndexByLabel(String labelName)
根据标签添加索引

使用：call chineseFulltextIndex.addNodeIndexByLabel("labelName")

### chineseFulltextIndex.addNodesIndex
为所有标签添加索引

使用：call chineseFulltextIndex.addNodesIndex

### chineseFulltextIndex.addNodesIndexByLabels（List<String> labelNames）
为一部分标签添加索引

使用：call chineseFulltextIndex.addNodesIndexByLabels（["labelName1", "labelName2"...]）

### chineseFulltextIndex.queryByProperty（String labelName, List<String> propKeys, String value）
在某个标签的某些属性上查询

使用：call chineseFulltextIndex.queryByProperty（"labelName", ["propKey1","propKey2"...], "value"）

### chineseFulltextIndex.queryByLabel（List<String> labelNames, String value）
在一部分标签的所有属性上查询

使用：call chineseFulltextIndex.queryByLabel（["labelName1", "labelName2"...], "value"）

### chineseFulltextIndex.queryByValue（String value）
在所有标签的所有属性上查询

使用：call chineseFulltextIndex.queryByValue（"value"）

### chineseFulltextIndex.addNodesIndexByProperties(List<String> properties)
在所有需要的属性上添加索引

使用:call chineseFulltextIndex.addNodesIndexByProperties(["Property1", "Property2"...])