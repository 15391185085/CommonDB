/*
 * Copyright (c) 2013. wyouflf (wyouflf@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ieds.gis.base.dao;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.lidroid.xutils.db.sqlite.WhereBuilder;
import com.lidroid.xutils.db.table.MyTable;
import com.lidroid.xutils.exception.DbException;
import com.lidroid.xutils.util.StringUtil;

/**
 * Author: wyouflf Date: 13-8-9 Time: 下午10:19
 */
public class Selector implements ISelector {

	protected Class<?> entityType;
	protected String tableName;

	protected WhereBuilder whereBuilder;
	protected List<OrderBy> orderByList;
	protected int limit = 0;
	protected int offset = 0;

	public WhereBuilder getWhereBuilder() {
		return whereBuilder;
	}

	public Selector(Class<?> entityType) throws DbException {
		this.entityType = entityType;
		this.tableName = MyTable.get(entityType).getTableName();
	}

	public static Selector from(Class<?> entityType) throws DbException {
		return new Selector(entityType);
	}

	public Selector where(WhereBuilder whereBuilder) {
		this.whereBuilder = whereBuilder;
		return this;
	}

	public Selector where(String columnName, String op, Object value) {
		this.whereBuilder = WhereBuilder.b(columnName, op, value);
		return this;
	}

	public Selector and(String columnName, String op, Object value) {
		this.whereBuilder.append(columnName, op, value);
		return this;
	}

	public Selector or(String columnName, String op, Object value) {
		this.whereBuilder.appendOR(columnName, op, value);
		return this;
	}

	public Selector orderBy(String columnName) {
		if (orderByList == null) {
			orderByList = new ArrayList<OrderBy>(2);
		}
		orderByList.add(new OrderBy(columnName));
		return this;
	}

	public Selector orderBy(String columnName, boolean desc) {
		if (orderByList == null) {
			orderByList = new ArrayList<OrderBy>(2);
		}
		orderByList.add(new OrderBy(columnName, desc));
		return this;
	}

	public Selector limit(int limit) {
		this.limit = limit;
		return this;
	}

	public Selector offset(int offset) {
		this.offset = offset;
		return this;
	}

	// @Override
	// public String toString() {
	// StringBuilder result = new StringBuilder();
	// result.append("SELECT ");
	// result.append("*");
	// result.append(" FROM ").append(tableName);
	// if (whereBuilder != null) {
	// result.append(" WHERE ").append(whereBuilder.toString());
	// }
	// if (orderByList != null) {
	// for (int i = 0; i < orderByList.size(); i++) {
	// result.append(" ORDER BY ").append(orderByList.get(i).toString());
	// }
	// }
	// if (limit > 0) {
	// result.append(" LIMIT ").append(limit);
	// result.append(" OFFSET ").append(offset);
	// }
	// return result.toString();
	// }

	public String getSelectSql() {
		StringBuilder fieldBuilder = new StringBuilder();

		Field[] fs = entityType.getDeclaredFields();
		for (Field f : fs) {
			fieldBuilder.append(f.getName() + ",");
		}
		String fb = StringUtil.deleteLastCharacter(fieldBuilder);
		StringBuilder result = new StringBuilder();
		result.append("SELECT ");
		result.append(fb);
		result.append(" FROM ").append(tableName);
		if (whereBuilder != null) {
			result.append(" WHERE ").append(whereBuilder.toString());
		}
		if (orderByList != null) {
			for (int i = 0; i < orderByList.size(); i++) {
				result.append(" ORDER BY ").append(
						orderByList.get(i).toString());
			}
		}
		if (limit > 0) {
			result.append(" LIMIT ").append(limit);
			result.append(" OFFSET ").append(offset);
		}
		return result.toString();
	}

	public Class<?> getEntityType() {
		return entityType;
	}

	protected class OrderBy {
		private String columnName;
		private boolean desc;

		public OrderBy(String columnName) {
			this.columnName = columnName;
		}

		public OrderBy(String columnName, boolean desc) {
			this.columnName = columnName;
			this.desc = desc;
		}

		@Override
		public String toString() {
			return columnName + (desc ? " DESC" : " ASC");
		}
	}
}
