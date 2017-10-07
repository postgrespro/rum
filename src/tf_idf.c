/*-------------------------------------------------------------------------
 *
 * tf_idf.c
 *		Implementation of TD/IDF statistics calculation.
 *
 * Portions Copyright (c) 2017, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"

#include "rum.h"

char *TFIDFSource;

#define EXIT_CHECK_TF_IDF_SOURCE(error) \
	do { \
		GUC_check_errdetail(error); \
		pfree(rawname); \
		list_free(namelist); \
		if (rel) \
			RelationClose(rel); \
		return false; \
	} while (false);

bool
check_tf_idf_source(char **newval, void **extra, GucSource source)
{
	char	   *rawname;
	char	   *attname;
	List	   *namelist;
	Oid			namespaceId;
	Oid			relId;
	Relation	rel = NULL;
	TupleDesc	tupDesc;
	AttrNumber	attrno;
	int			i;

	/* Need a modifiable copy of string */
	rawname = pstrdup(*newval);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawname, '.', &namelist))
	{
		/* syntax error in name list */
		EXIT_CHECK_TF_IDF_SOURCE("List syntax is invalid.");
	}

	switch (list_length(namelist))
	{
		case 0:
			return true;
		case 1:
			EXIT_CHECK_TF_IDF_SOURCE("improper column name (there should be at least 2 dotted names)");
		case 2:
			relId = RelnameGetRelid(linitial(namelist));
			attname = lsecond(namelist);
			break;
		case 3:
			/* use exact schema given */
			namespaceId = LookupExplicitNamespace(linitial(namelist), true);
			if (!OidIsValid(namespaceId))
				relId = InvalidOid;
			else
				relId = get_relname_relid(lsecond(namelist), namespaceId);
			attname = lthird(namelist);
			break;
		default:
			EXIT_CHECK_TF_IDF_SOURCE("improper column name (too many dotted names)");
	}

	if (!OidIsValid(relId))
		EXIT_CHECK_TF_IDF_SOURCE("relation not found");

	rel = RelationIdGetRelation(relId);
	tupDesc = rel->rd_att;
	if (rel->rd_rel->relkind == RELKIND_INDEX)
	{
		attrno = pg_atoi(attname, sizeof(attrno), 10);
		if (attrno <= 0 || attrno > rel->rd_index->indnatts)
			EXIT_CHECK_TF_IDF_SOURCE("wrong index attribute number");
		if (rel->rd_index->indkey.values[attrno - 1] != InvalidAttrNumber)
			EXIT_CHECK_TF_IDF_SOURCE("regular indexed column is specified");
	}
	else
	{
		attrno = InvalidAttrNumber;
		for (i = 0; i < tupDesc->natts; i++)
		{
			if (namestrcmp(&(tupDesc->attrs[i]->attname), attname) == 0)
			{
				attrno = tupDesc->attrs[i]->attnum;
				break;
			}
		}

		if (attrno == InvalidAttrNumber)
			EXIT_CHECK_TF_IDF_SOURCE("attribute not found");
	}

	if (tupDesc->attrs[attrno - 1]->atttypid != TSVECTOROID)
		EXIT_CHECK_TF_IDF_SOURCE("attribute should be of tsvector type");

	pfree(rawname);
	list_free(namelist);
	RelationClose(rel);
	return true;
}


void
assign_tf_idf_source(const char *newval, void *extra)
{

}