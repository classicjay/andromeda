[
 <#--切换标签-->
  <#if dataList??>
    <#list dataList as dataMap>
     {
      <#list dataMap?keys as key>
      	"${key}":"${dataMap[key]}"
      	<#if key_has_next>,</#if>
      </#list>
     }
     <#if dataMap_has_next>,</#if>
    </#list>
  </#if>
]

