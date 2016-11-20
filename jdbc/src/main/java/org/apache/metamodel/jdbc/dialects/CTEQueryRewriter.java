package org.apache.metamodel.jdbc.dialects;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.OrderByClause;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectClause;
import org.apache.metamodel.query.parser.QueryParserException;

public class CTEQueryRewriter extends DefaultQueryRewriter {

	public CTEQueryRewriter(JdbcDataContext dataContext) {
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
	 * {@inheritDoc} Using the CTE query rewriting technique for SQL Servers
	 * 2008 and below.
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

		if (hasLimit) {
			if (firstRow == null) {
				firstRow = 0;
			}

			if (maxRows == null) {
				maxRows = Integer.MAX_VALUE;
			}
			
			String select = super.rewriteSelectClause(query, query.getSelectClause()).trim();
			String orderBy = super.rewriteOrderByClause(query, query.getOrderByClause()).trim();

			queryString = queryString.replaceFirst(select, "");
			queryString = queryString.replaceFirst(orderBy, "").trim();

			queryString = String.format("%s, ROW_NUMBER() OVER(%s) AS RowNum %s", select, orderBy, queryString);

			queryString = String.format(";WITH QR AS (%s) SELECT * FROM QR WHERE RowNum BETWEEN %d AND %d", queryString, firstRow, maxRows);
		}

		return queryString;
	}

	@Override
	protected String rewriteOrderByClause(Query query, OrderByClause orderByClause) {
		String orderByClauseString = super.rewriteOrderByClause(query, orderByClause);

		if (this.hasCTERewrite(query)) {
			orderByClauseString = "";
		}

		return orderByClauseString;
	}

	@Override
	protected String rewriteSelectClause(Query query, SelectClause selectClause) {
		String selectClauseString = super.rewriteSelectClause(query, selectClause);

		if (this.hasCTERewrite(query)) {
			selectClauseString = "";
		}

		return selectClauseString;
	}

	private boolean hasCTERewrite(Query query) {
		Integer maxRows = query.getMaxRows();
		Integer firstRow = query.getFirstRow();

		boolean hasOrderBy = !query.getOrderByClause().isEmpty();
		boolean hasLimit = maxRows != null || firstRow != null;

		return hasOrderBy && hasLimit;
	}

}
