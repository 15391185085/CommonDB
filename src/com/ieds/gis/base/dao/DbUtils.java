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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;

import com.lidroid.xutils.db.sqlite.CursorUtils;
import com.lidroid.xutils.db.sqlite.SqlInfo;
import com.lidroid.xutils.db.sqlite.SqlInfoBuilder;
import com.lidroid.xutils.db.sqlite.WhereBuilder;
import com.lidroid.xutils.db.table.MyColumn;
import com.lidroid.xutils.db.table.MyId;
import com.lidroid.xutils.db.table.KeyValue;
import com.lidroid.xutils.db.table.MyTable;
import com.lidroid.xutils.exception.DbException;
import com.lidroid.xutils.util.IOUtils;
import com.lidroid.xutils.util.LogUtils;
import com.lidroid.xutils.util.StringUtil;

/**
 * !SHAPE.JSON! po类必须有构造函数 带外键的表不能用复合主键
 * 
 * 注意: 复合主键需要用saveOrUpdateDoubleKey来做“增，改”的业务 复合主键需要用deleteDoubleKey来做“删除”的业务
 * 
 * @update 2014-11-12 上午11:08:31<br>
 * @author <a href="mailto:lihaoxiang@ieds.com.cn">李昊翔</a>
 * 
 */
public abstract class DbUtils implements IDbUtils {
	/**
	 * 创建数据库时调用 版本号从0开始进行更新 检查到高于当前版本时进行更新 更新后自动保存版本号到最新给定的值
	 */
	private static final int DATABASE_INIT = 0;
	public static final String NOT_WHERE = "参数没有定义";
	private SQLiteDatabase database;
	private boolean debug = false;
	private boolean allowTransaction = false;

	public DbUtils(File dbFile, int mNewVersion) {
		if (mNewVersion < 1)
			throw new IllegalArgumentException("Version must be >= 1, was "
					+ mNewVersion);
		// 允许交易
		this.configAllowTransaction(true);
		// 允许打印日志
		this.configDebug(true);
		this.database = getSQLiteDatabase(dbFile, mNewVersion);
	}

	public SQLiteDatabase getSQLiteDatabase(File dbFile, int mNewVersion) {
		SQLiteDatabase db = SQLiteDatabase.openOrCreateDatabase(dbFile, null);

		final int version = db.getVersion();
		if (version != mNewVersion) {
			if (db.isReadOnly()) {
				throw new SQLiteException(
						"Can't upgrade read-only database from version "
								+ db.getVersion() + " to " + mNewVersion + ": "
								+ dbFile.getName());
			}

			db.beginTransaction();
			try {
				if (version == DATABASE_INIT) {
					onCreate(db);
				} else {
					if (version > mNewVersion) {
						onDowngrade(db, version, mNewVersion);
					} else {
						onUpgrade(db, version, mNewVersion);
					}
				}
				db.setVersion(mNewVersion);
				db.setTransactionSuccessful();
			} finally {
				db.endTransaction();
			}
		}

		return db;
	}

	public abstract void onCreate(SQLiteDatabase db);

	public abstract void onUpgrade(SQLiteDatabase db, int oldVersion,
			int newVersion);

	public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		throw new SQLiteException("Can't downgrade database from version "
				+ oldVersion + " to " + newVersion);
	}

	public DbUtils configDebug(boolean debug) {
		this.debug = debug;
		return this;
	}

	public DbUtils configAllowTransaction(boolean allowTransaction) {
		this.allowTransaction = allowTransaction;
		return this;
	}

	public SQLiteDatabase getDatabase() {
		return database;
	}

	/**
	 * @param entity
	 * @return
	 * @throws DbException
	 */
	public <T> T findFirstByIdEnableNull(T entity) throws DbException {
		Selector selector = getSelectorById(entity);
		T t = findFirstEnableNull(selector);
		return t;
	}

	/**
	 * @param entity
	 * @return
	 * @throws DbException
	 */
	public <T> T findFirstById(T entity) throws DbException {
		Selector selector = getSelectorById(entity);
		T t = findFirstEnableNull(selector);
		return getFindCheck(selector, t);
	}

	public void deleteById(Object entity) throws DbException {
		try {
			beginTransaction();

			deleteWithoutTransactionById(entity);

			setTransactionSuccessful();
		} finally {
			endTransaction();
		}
	}

	public <T> void deleteById(List<T> entities) throws DbException {
		if (entities == null || entities.size() < 1)
			return;
		try {
			beginTransaction();

			for (Object entity : entities) {
				if (entity != null) {
					deleteWithoutTransactionById(entity);
				}
			}

			setTransactionSuccessful();
		} finally {
			endTransaction();
		}
	}

	private void deleteWithoutTransactionById(Object entity) throws DbException {
		SqlInfo result = new SqlInfo();
		Selector selector = getSelectorById(entity);
		if (selector.getWhereBuilder() == null) {
			throw new DbException(DbUtils.getSqlError(NOT_WHERE, selector
					.limit(1).getSelectSql()));
		}
		SqlInfo sql = SqlInfoBuilder.buildDeleteSqlInfo(entity.getClass(),
				selector.getWhereBuilder());
		result.setSql(sql.getSql());
		execNonQuery(result);
	}

	public void replace(Object entity) throws DbException {
		try {
			beginTransaction();

			replaceWithoutTransaction(entity);

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public <T> void replace(List<T> entities) throws DbException {
		try {
			beginTransaction();

			for (Object entity : entities) {
				if (entity != null) {
					replaceWithoutTransaction(entity);
				}
			}

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public void ignore(Object entity) throws DbException {
		try {
			beginTransaction();

			ignoreWithoutTransaction(entity);

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public <T> void ignore(List<T> entities) throws DbException {
		try {
			beginTransaction();

			for (Object entity : entities) {
				if (entity != null) {
					ignoreWithoutTransaction(entity);
				}
			}

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public void save(Object entity) throws DbException {
		try {
			beginTransaction();

			saveWithoutTransaction(entity);

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public <T> void save(List<T> entities) throws DbException {
		try {
			beginTransaction();

			for (Object entity : entities) {
				if (entity != null) {
					saveWithoutTransaction(entity);
				}
			}

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public void delete(Object entity) throws DbException {
		try {
			beginTransaction();

			deleteWithoutTransaction(entity);

			setTransactionSuccessful();
		} finally {
			endTransaction();
		}
	}

	public <T> void delete(List<T> entities) throws DbException {
		if (entities == null || entities.size() < 1)
			return;
		try {
			beginTransaction();

			for (Object entity : entities) {
				if (entity != null) {
					deleteWithoutTransaction(entity);
				}
			}

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public void delete(Class<?> entityType, WhereBuilder whereBuilder)
			throws DbException {
		try {
			beginTransaction();

			SqlInfo sql = SqlInfoBuilder.buildDeleteSqlInfo(entityType,
					whereBuilder);
			execNonQuery(sql);

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	/**
	 * 根据id更新对象数据
	 * 
	 * @param entity
	 * @throws DbException
	 */
	public void updateById(Object entity) throws DbException {
		try {
			beginTransaction();

			updateWithoutTransaction(entity);

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	/**
	 * 根据id更新所有对象数据
	 * 
	 * @param entity
	 * @throws DbException
	 */
	public <T> void updateById(List<T> entities) throws DbException {
		if (entities == null || entities.size() < 1)
			return;
		try {
			beginTransaction();

			for (Object entity : entities) {
				if (entity != null) {
					updateWithoutTransaction(entity);
				}
			}

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	public void updateByWhere(Object entity, WhereBuilder whereBuilder)
			throws DbException {
		try {
			beginTransaction();

			execNonQuery(SqlInfoBuilder
					.buildUpdateSqlInfo(entity, whereBuilder));

			setTransactionSuccessful();
		} finally {
			endTransaction();

		}
	}

	/**
	 * 查询结果不能为空，为空时抛出异常
	 * 
	 * @param selector
	 * @return
	 * @throws DbException
	 */
	public <T> T findFirst(ISelector selector) throws DbException {
		T t = findFirstEnableNull(selector);
		return getFindCheck(selector, t);
	}

	/**
	 * @param selector
	 * @param t
	 * @return
	 * @throws DbException
	 */
	private <T> T getFindCheck(ISelector selector, T t) throws DbException {
		if (t == null) {
			throw new DbException("查询不到有效数据,sql=("
					+ selector.limit(1).getSelectSql() + ")");
		} else {
			return t;
		}
	}

	/**
	 * 查询结果不能为空，为空时抛出异常
	 * 
	 * @param entity
	 * @return
	 * @throws DbException
	 */
	public <T> T findFirst(Object entity) throws DbException {
		Selector selector = getSelector(entity);
		return findFirst(selector);
	}

	/**
	 * 查询结果不能为空，为空时抛出异常
	 * 
	 * @param entity
	 * @return
	 * @throws DbException
	 */
	public <T> T findFirstEnableNull(Object entity) throws DbException {
		Selector selector = getSelector(entity);
		return findFirstEnableNull(selector);
	}

	/**
	 * 查询结果可以为空
	 * 
	 * @param selector
	 * @return
	 * @throws DbException
	 */
	public <T> T findFirstEnableNull(ISelector selector) throws DbException {
		if (selector.getWhereBuilder() == null) {
			throw new DbException(getSqlError(NOT_WHERE, selector.limit(1)
					.getSelectSql()));
		}
		String sql = selector.limit(1).getSelectSql();
		Cursor cursor = execQuery(sql);
		try {
			if (cursor.moveToNext()) {
				T entity = (T) CursorUtils.getEntity(cursor,
						selector.getEntityType());
				return entity;
			}
		} finally {
			IOUtils.closeQuietly(cursor);

		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public <T> List<T> findAll(ISelector selector) throws DbException {
		String sql = selector.getSelectSql();
		Cursor cursor = execQuery(sql);
		List<T> result = new ArrayList<T>();
		try {
			while (cursor.moveToNext()) {
				T entity = (T) CursorUtils.getEntity(cursor,
						selector.getEntityType());
				result.add(entity);
			}
		} finally {
			IOUtils.closeQuietly(cursor);

		}
		return result;
	}

	public <T> List<T> findAll(Object entity) throws DbException {
		Selector selector = getSelector(entity);
		return findAll(selector);
	}

	/**
	 * 根据对象的属性查询该对象的完整属性
	 * 
	 * @param entity
	 * @return
	 * @throws DbException
	 */
	public Selector getSelector(Object entity) throws DbException {
		Selector selector = Selector.from(entity.getClass());
		List<KeyValue> entityKvList = SqlInfoBuilder
				.entityKeyAndValueList(entity);
		if (entityKvList != null && !entityKvList.isEmpty()) {
			WhereBuilder wb = WhereBuilder.b();
			for (KeyValue keyValue : entityKvList) {
				wb.append(keyValue.getKey(), "=", keyValue.getValue());
			}
			selector.where(wb);
		}
		return selector;
	}

	/**
	 * 根据对象的id属性查询该对象的完整属性
	 * 
	 * @param entity
	 * @return
	 */
	public Selector getSelectorById(Object entity) throws DbException {
		Selector selector = Selector.from(entity.getClass());
		MyTable table = MyTable.get(entity.getClass());
		List<MyId> idList = table.getId();
		if (idList != null && !idList.isEmpty()) {
			WhereBuilder wb = WhereBuilder.b();
			for (int i = 0; i < idList.size(); i++) {
				MyId id = (MyId) idList.get(i);
				Object idValue = id.getColumnValue(entity);

				if (idValue == null) {
					throw new DbException("对象[" + entity.getClass()
							+ "]的id不能是null");
				}
				wb.append(id.getColumnName(), "=", idValue);
			}
			selector.where(wb);
		}
		return selector;
	}

	private void replaceWithoutTransaction(Object entity) throws DbException {
		execNonQuery(SqlInfoBuilder.buildReplaceSqlInfo(entity));
	}

	private void saveWithoutTransaction(Object entity) throws DbException {
		execNonQuery(SqlInfoBuilder.buildInsertSqlInfo(entity));
	}

	private void ignoreWithoutTransaction(Object entity) throws DbException {
		execNonQuery(SqlInfoBuilder.buildIgnoreSqlInfo(entity));
	}

	private void deleteWithoutTransaction(Object entity) throws DbException {
		SqlInfo result = new SqlInfo();
		List<KeyValue> entityKvList = SqlInfoBuilder
				.entityKeyAndValueList(entity);
		WhereBuilder wb = null;
		if (entityKvList != null && !entityKvList.isEmpty()) {
			wb = WhereBuilder.b();
			for (KeyValue keyValue : entityKvList) {
				wb.append(keyValue.getKey(), "=", keyValue.getValue());
			}
		}
		SqlInfo sql = SqlInfoBuilder.buildDeleteSqlInfo(entity.getClass(), wb);
		result.setSql(sql.getSql());
		execNonQuery(result);
	}

	private void updateWithoutTransaction(Object entity) throws DbException {
		execNonQuery(SqlInfoBuilder.buildUpdateSqlInfo(entity));
	}

	// ************************************************ tools
	// ***********************************

	private static void fillContentValues(ContentValues contentValues,
			List<KeyValue> list) {
		if (list != null && contentValues != null) {
			for (KeyValue kv : list) {
				contentValues.put(kv.getKey(), kv.getValue().toString());
			}
		} else {
			LogUtils.w("List<KeyValue> is empty or ContentValues is empty!");
		}
	}

	public void dropDb() throws DbException {
		Cursor cursor = null;
		try {
			cursor = execQuery("SELECT name FROM sqlite_master WHERE type ='table'");
			if (cursor != null) {
				while (cursor.moveToNext()) {
					try {
						execNonQuery("DROP TABLE " + cursor.getString(0));
					} catch (Exception e) {
						throw new DbException(e.getMessage());
					}
				}
			}
		} finally {
			IOUtils.closeQuietly(cursor);

		}
	}

	public void dropTable(Class<?> entityType) throws DbException {
		MyTable table = MyTable.get(entityType);
		execNonQuery("DROP TABLE " + table.getTableName());
	}

	// /////////////////////////////////// exec sql
	// /////////////////////////////////////////////////////
	private void debugSql(String sql) {
		if (debug) {
			LogUtils.d(sql);
		}
	}

	public void beginTransaction() {
		if (allowTransaction) {
			database.beginTransaction();
		}
	}

	public void setTransactionSuccessful() {
		if (allowTransaction) {
			database.setTransactionSuccessful();
		}
	}

	public void endTransaction() {
		if (allowTransaction) {
			database.endTransaction();
		}
	}

	public void execNonQuery(SqlInfo sqlInfo) throws DbException {
		debugSql(sqlInfo.getSql());
		try {
			if (sqlInfo.getBindArgs() != null) {
				database.execSQL(sqlInfo.getSql(), sqlInfo.getBindArgsAsArray());
			} else {
				database.execSQL(sqlInfo.getSql());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DbException(getSqlError(e.getMessage(), sqlInfo.getSql(),
					sqlInfo.getBindArgsAsArray()));
		}
	}

	public void execNonQuery(String sql) throws DbException {
		debugSql(sql);
		try {
			database.execSQL(sql);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DbException(getSqlError(e.getMessage(), sql));
		}
	}

	public Cursor execQuery(SqlInfo sqlInfo) throws DbException {
		debugSql(sqlInfo.getSql());
		try {
			return database.rawQuery(sqlInfo.getSql(),
					sqlInfo.getBindArgsAsStrArray());
		} catch (Exception e) {
			e.printStackTrace();
			throw new DbException(getSqlError(e.getMessage(), sqlInfo.getSql(),
					sqlInfo.getBindArgsAsStrArray()));
		}
	}

	public Cursor execQuery(String sql) throws DbException {
		debugSql(sql);
		try {
			return database.rawQuery(sql, null);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DbException(getSqlError(e.getMessage(), sql));
		}
	}

	/**
	 * @param sql
	 * @return
	 */
	private static String getSqlError(String message, String sql, Object[] array) {
		String a = "无";
		String s = "无";
		if (sql != null) {
			s = sql;
		}

		if (array != null) {
			StringBuilder sb = new StringBuilder();
			for (Object b : array) {
				sb.append(b.toString() + ",");
			}
			a = StringUtil.deleteLastCharacter(sb);
		}
		return "异常原因：" + message + "\n异常语句：sql=(" + s + "),参数=(" + a + ")";

	}

	private static String getSqlError(String message, String sql) {
		return getSqlError(message, sql, null);
	}

	@Override
	public ISelector from(Class<?> entityType) throws DbException {
		// TODO Auto-generated method stub
		return new Selector(entityType);
	}

	@Override
	public List<List<String>> execListQuery(SqlInfo sqlInfo) throws DbException {
		List<List<String>> l = new ArrayList<List<String>>();
		Cursor cursor = execQuery(sqlInfo);
		try {
			while (cursor.moveToNext()) {
				ArrayList<String> a1 = new ArrayList<String>();
				for (int i = 0; i < cursor.getColumnCount(); i++) {
					a1.add(cursor.getString(i));
				}
				l.add(a1);
			}
		} finally {
			IOUtils.closeQuietly(cursor);

		}
		return l;
	}

	@Override
	public List<List<String>> execListQuery(String sql) throws DbException {
		List<List<String>> l = new ArrayList<List<String>>();
		Cursor cursor = execQuery(sql);
		try {
			while (cursor.moveToNext()) {
				ArrayList<String> a1 = new ArrayList<String>();
				for (int i = 0; i < cursor.getColumnCount(); i++) {
					a1.add(cursor.getString(i));
				}
				l.add(a1);
			}
		} finally {
			IOUtils.closeQuietly(cursor);

		}
		return l;
	}
}
