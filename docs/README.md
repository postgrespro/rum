# Extensibility

The RUM interface has a high level of abstraction, requiring the access method implementer only to implement the semantics of the data type being accessed. The GIN layer itself takes care of concurrency, logging and searching the tree structure.

All it takes to get a RUM access method working is to implement a few user-defined methods, which define the behavior of keys in the tree and the relationships between keys, indexed items, and indexable queries. In short, RUM combines extensibility with generality, code reuse, and a clean interface.

## List of RUM operator class support functions

|Number|Function and parameters|
|------|-----------------------|
|FUNCTION        1       |rum_compare(STORAGETYPE, STORAGETYPE)|
|FUNCTION        2       |rum_extract_value(VALUETYPE,internal,internal,internal,internal)|
|FUNCTION        3       |rum_extract_query(QUERYTYPE,internal,smallint,internal,internal,internal,internal)|
|FUNCTION        4       |rum_consistent(internal,smallint,VALUETYPE,int,internal,internal,internal,internal)|
|FUNCTION        5       |rum_compare_prefix(VALUETYPE,VALUETYPE,smallint,internal)|
|FUNCTION        6       |rum_config(internal)|
|FUNCTION        7       |rum_pre_consistent(internal,smallint,VALUETYPE,int,internal,internal,internal,internal)|
|FUNCTION        8       |rum_ordering_distance(internal,smallint,VALUETYPE,int,internal,internal,internal,internal,internal)|
|FUNCTION        9       |rum_outer_distance(VALUETYPE, VALUETYPE, smallint)|
|FUNCTION        10      |rum_join_pos(internal, internal)|

### Function 1 rum_compare

```
        int rum_compare(Datum a, Datum b)
```

### Function 2 rum_extract_value
```        
        Datum *rum_extract_value(Datum itemValue, int32 *nkeys, bool **nullFlags, Datum **addInfo, bool **nullFlagsAddInfo)
```
Returns a palloc'd array of keys given an item to be indexed. The number of returned keys must be stored into `*nkeys`. If any of the keys can be null, also palloc an array of `*nkeys` bool fields, store its address at `*nullFlags`, and set these null flags as needed. `*nullFlags` can be left NULL (its initial value) if all keys are non-null. The return value can be NULL if the item contains no keys.

### Function 3 rum_extract_query
```        
        Datum *rum_extract_query(Datum query, int32 *nkeys, StrategyNumber n, bool **pmatch, Pointer **extra_data, 
         bool **nullFlags, int32 *searchMode)
```
Returns a palloc'd array of keys given a value to be queried; that is, query is the value on the right-hand side of an indexable operator whose left-hand side is the indexed column. `n` is the strategy number of the operator within the operator class (see Section [36.16.2](https://postgrespro.com/docs/postgrespro/15/xindex#XINDEX-STRATEGIES)). Often, rum_extract_query will need to consult `n` to determine the data type of query and the method it should use to extract key values. The number of returned keys must be stored into `*nkeys`. If any of the keys can be null, also palloc an array of `*nkeys` bool fields, store its address at `*nullFlags`, and set these null flags as needed. `*nullFlags` can be left NULL (its initial value) if all keys are non-null. The return value can be NULL if the query contains no keys.

`searchMode` is an output argument that allows rum_extract_query to specify details about how the search will be done. If `*searchMode` is set to GIN_SEARCH_MODE_DEFAULT (which is the value it is initialized to before call), only items that match at least one of the returned keys are considered candidate matches. If `*searchMode` is set to GIN_SEARCH_MODE_INCLUDE_EMPTY, then in addition to items containing at least one matching key, items that contain no keys at all are considered candidate matches. (This mode is useful for implementing is-subset-of operators, for example.) If `*searchMode` is set to GIN_SEARCH_MODE_ALL, then all non-null items in the index are considered candidate matches, whether they match any of the returned keys or not. (This mode is much slower than the other two choices, since it requires scanning essentially the entire index, but it may be necessary to implement corner cases correctly. An operator that needs this mode in most cases is probably not a good candidate for a RUM operator class.) The symbols to use for setting this mode are defined in rum.h and access/gin.h.

`pmatch` is an output argument for use when partial match is supported. To use it, rum_extract_query must allocate an array of `*nkeys` bools and store its address at `*pmatch`. Each element of the array should be set to true if the corresponding key requires partial match, false if not. If `*pmatch` is set to NULL then RUM assumes partial match is not required. The variable is initialized to NULL before call, so this argument can simply be ignored by operator classes that do not support partial match.

`extra_data` is an output argument that allows rum_extract_query to pass additional data to the consistent and comparePartial methods. To use it, rum_extract_query must allocate an array of `*nkeys` pointers and store its address at `*extra_data`, then store whatever it wants to into the individual pointers. The variable is initialized to NULL before call, so this argument can simply be ignored by operator classes that do not require extra data. If `*extra_data` is set, the whole array is passed to the consistent method, and the appropriate element to the comparePartial method.
### Function 4 rum_consistent
```         
        bool rum_consistent(bool check[], StrategyNumber n, Datum query, int32 nkeys, Pointer extra_data[], 
         bool *recheck, Datum queryKeys[], bool nullFlags[], Datum **addInfo, bool **nullFlagsAddInfo)
```
An operator class must also provide a function to check if an indexed item matches the query. 

Returns true if an indexed item satisfies the query operator with strategy number `n` (or might satisfy it, if the recheck indication is returned). This function does not have direct access to the indexed item's value, since RUM does not store items explicitly. Rather, what is available is knowledge about which key values extracted from the query appear in a given indexed item. The check array has length `nkeys`, which is the same as the number of keys previously returned by rum_extract_query for this query datum. Each element of the check array is true if the indexed item contains the corresponding query key, i.e., if `(check[i] == true)` the i-th key of the rum_extract_query result array is present in the indexed item. The original query datum is passed in case the consistent method needs to consult it, and so are the `queryKeys[]` and `nullFlags[]` arrays previously returned by rum_extract_query. `extra_data`` is the extra-data array returned by rum_extract_query, or NULL if none.

When rum_extract_query returns a null key in `queryKeys[]`, the corresponding `check[]` element is true if the indexed item contains a null key; that is, the semantics of `check[]` are like IS NOT DISTINCT FROM. The consistent function can examine the corresponding `nullFlags[]` element if it needs to tell the difference between a regular value match and a null match.

On success, `*recheck` should be set to true if the heap tuple needs to be rechecked against the query operator, or false if the index test is exact. That is, a false return value guarantees that the heap tuple does not match the query; a true return value with `*recheck` set to false guarantees that the heap tuple does match the query; and a true return value with `*recheck` set to true means that the heap tuple might match the query, so it needs to be fetched and rechecked by evaluating the query operator directly against the originally indexed item.
### Function 5 rum_compare_prefix
```         
        int32 rum_compare_prefix(Datum a, Datum b,StrategyNumber n,void *addInfo) 
```
Compares two keys (not indexed items!) and returns an integer less than zero, zero, or greater than zero, indicating whether the first key is less than, equal to, or greater than the second. Null keys are never passed to this function.

Alternatively, if the operator class does not provide a compare method, RUM will look up the default btree operator class for the index key data type, and use its comparison function. It is recommended to specify the comparison function in a RUM operator class that is meant for just one data type, as looking up the btree operator class costs a few cycles. However, polymorphic RUM operator classes (such as array_ops) typically cannot specify a single comparison function.

### Function 6 rum_config (used names: RUM_CONFIG_PROC configFn)
```        
        void rum_config(RumConfig *config)
```
Example:

```C
PG_FUNCTION_INFO_V1(rum_int8_config);									
Datum																	
rum_int8_config(PG_FUNCTION_ARGS)										
{																		
	RumConfig  *config = (RumConfig *) PG_GETARG_POINTER(0);
															
	config->addInfoTypeOid = InvalidOid;					
    /* or config->addInfoTypeOid = INT4OID; */
															
	config->strategyInfo[0].strategy = RUM_LEFT_DISTANCE;	
	config->strategyInfo[0].direction = BackwardScanDirection;
															
	config->strategyInfo[1].strategy = RUM_RIGHT_DISTANCE;	
	config->strategyInfo[1].direction = ForwardScanDirection;
	/* last element in array */														
	config->strategyInfo[2].strategy = InvalidStrategy;		
															
	PG_RETURN_VOID();										
}	
```

### Function 7 rum_pre_consistent (used names: RUM_PRE_CONSISTENT_PROC preConsistentFn)
```
        bool rum_pre_consistent(bool check[], StrategyNumber n, Datum query, int32 nkeys, Pointer extra_data[], 
         bool *recheck, Datum queryKeys[], bool nullFlags[])
         
        Return value:
         false if index key value is not consistent with query
         true if key value MAYBE consistent and will be rechecked in rum_consistent function
        Parameters:
         check[] contains false, if element is not match any query element. 
                 if all elements in check[] equal to true, this function is not called
                 if all elements in check[] equal to false, this function is not called 
         StrategyNumber - the number of the strategy from the class operator        
         query
         nkeys quantity of elements in check[]. If nkeys==0, this function is not called
         recheck - parameter is not used
         queryKeys[] - returned by rum_extract_query
         nullFlags[] - returned by rum_extract_query (or all false, if rum_extract_query did not return nullFlags)  
```
If operator class defines rum_pre_consistent function, it is called before rum_consistent (if search mode equals to GIN_SEARCH_MODE_DEFAULT and not all elements in check[] equal to true).

### Function 8 rum_ordering_distance (used names: RUM_ORDERING_PROC orderingFn)
```
        double rum_ordering_distance(bool check[], ?StrategyNumber n, ?Datum query, int32 nkeys, ?Pointer extra_data[], 
         ?bool *recheck, ?Datum queryKeys[], ?bool nullFlags[], Datum **addInfo, bool **nullFlagsAddInfo)
        double rum_ordering_distance(Datum curKey, Datum query, StrategyNumber n) 
```
Operator class defines rum_ordering_distance function, if it can provide distance based on index data. If distance calculations can be done using key and query value, 3 parameters function should be defined. If distance calculation can be done using addInfo data (f.e, if addInfo sores value of array size needed for distance calculation), 10 parameters function should be defined.


### Function 9 rum_outer_distance (used names: RUM_OUTER_ORDERING_PROC outerOrderingFn)
```
        double rum_outer_distance(Datum outerAddInfo, Datum query, StrategyNumber n)
```
Similar to rum_ordering_distance for indexes with attached fields.


### Function 10 rum_join_pos (used names: RUM_ADDINFO_JOIN joinAddInfoFn)
```         
        Datum rum_join_pos(Datum collected_item_addInfo, Datum current_collected_item_addInfo)
```


