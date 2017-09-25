{
   <#-- 表格数据-->
    <#if kpiMap??>
     <#list kpiMap?keys as key>
      <#if key="thData">
      "${key}":
         [
           <#assign titleList = kpiMap[key]>
              <#list titleList as title>
                "${title}"
               <#if title_has_next>,</#if>
              </#list>
          ]
       </#if>
       <#if key="tbodyData">
       "${key}":
       [
        <#assign datalist = kpiMap[key]>
           <#list datalist as dataMap>
           		{
				<#list dataMap?keys as key>
           			<#if key =="kpiValues">
           			"kpiValues":
           			[
		           	<#assign kpiValueList = dataMap[key]>
		              <#list kpiValueList as value>
		                "${value}"
		               <#if value_has_next>,</#if>
		              </#list>           				
           			]
           			<#elseif key =="histogramData" || key =="lineChartData">
           			"${key}":
	           			{
	           				<#assign chartMap = dataMap[key]>
	           				<#list chartMap?keys as key>
	           					"${key}":
							         [
							           <#assign chartMapList = chartMap[key]>
							              <#list chartMapList as value>
							               <#if key== "xData">
							               "${value}"
							               </#if>
							               <#if key== "value">
							               ${value}
							               </#if>
							               <#if value_has_next>,</#if>
							              </#list>
							          ]
	          				<#if key_has_next>,</#if>
			           		</#list>		 	           				
	           			}          			
           			<#elseif key=="ringRatio"||key=="identicalRatio">
           			"${key}":${dataMap[key]}
           			<#else>
           			"${key}":"${dataMap[key]}"
					</#if>
					<#if key_has_next>,</#if>
           		</#list>           			
           		}
            <#if dataMap_has_next>,</#if>
           </#list>
       ]
       </#if>
      <#if key_has_next>,</#if>
     </#list>
    </#if>
}
 
   
         
