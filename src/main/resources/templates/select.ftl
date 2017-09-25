[
<#--筛选条件-->
<#if resList??>
      <#list resList as selectMap>
         {
        <#list selectMap?keys as key>
          <#if key!="values">
          "${key}":"${selectMap[key]}"
          </#if>
           <#if key="values">
          "${key}":
            [
            <#assign dataList=selectMap["values"]>
             <#list dataList as dataMap>
               {
                <#list dataMap?keys as key>
                  "${key}":"${dataMap[key]}"
                  <#if key_has_next>,</#if>
                </#list>
               }
               <#if dataMap_has_next>,</#if>
             </#list>
            ]
          </#if>
          <#if key_has_next>,</#if>
        </#list>       
       }
         <#if selectMap_has_next>,</#if>
      </#list>
</#if>
]