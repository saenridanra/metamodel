package org.apache.metamodel.jdbc.dialects;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.parser.QueryParserException;

public class OffsetFetchQueryRewriter extends DefaultQueryRewriter {

	public OffsetFetchQueryRewriter(JdbcDataContext dataContext) {
		super(dataContext);
	}

	@Override
	public final boolean isFirstRowSupported() {
		return true;
	}

	@Override
	public final boolean isMaxRowsSupported() {
		return true;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * If the Max rows and/or First row property of the query is set, then we
	 * will use the database's OFFSET and FETCH functions.
	 * 
	 * This only works, if the ORDER BY Clause is set.
	 */
	@Override
	public String rewriteQuery(Query query) {
		String queryString = super.rewriteQuery(query);
		Integer maxRows = query.getMaxRows();
		Integer firstRow = query.getFirstRow();

		boolean hasOrderBy = !query.getOrderByClause().isEmpty();
		boolean hasLimit = maxRows != null || firstRow != null;

		if (!hasOrderBy && hasLimit) {
			throw new QueryParserException(
					"When using Sql Server and paging using" + " OFFSET / FETCH, an ORDER BY CLAUSE must be given.");
		}

		if (maxRows != null || firstRow != null) {
			if (firstRow == null) {
				firstRow = 0;
			} else {
				firstRow--;
			}

			// offset is 0-based
			queryString = queryString + " OFFSET " + firstRow + " ROWS";

			if (maxRows == null) {
				maxRows = Integer.MAX_VALUE;
			}
			queryString = queryString + " FETCH NEXT " + maxRows + " ROWS ONLY";
		}

		return queryString;
	}

}
