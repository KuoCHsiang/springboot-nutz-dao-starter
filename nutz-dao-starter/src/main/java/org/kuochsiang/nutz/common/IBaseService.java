package org.kuochsiang.nutz.common;

import org.nutz.dao.*;
import org.nutz.dao.pager.Pager;

import java.util.List;

public interface IBaseService<T> {
    Dao dao();

    int count(Condition cnd);

    int count();

    int count(String tableName, Condition cnd);

    int count(String tableName);

    public int func(String funcName, String fieldName);

    public int func(String funcName, String fieldName, Condition cnd);

    public int getMaxId();

    public T fetch(Long id);

    public T fetch(String id);

    public T fetch(Long id, String fieldName);

    public T fetch(String id, String fieldName);


    public <T> T fetchLinks(T obj, String regex);

    public <T> T fetchLinks(T obj, String regex, Condition cnd);

    public <T> T fetchLinks(T obj, String regex, FieldFilter fieldFilter);

    public T fetch(Condition cnd);

    public <T> T insert(T obj);

    public <T> T insertOrUpdate(T obj);

    public <T> T insertOrUpdate(T obj, FieldFilter insertFieldFilter, FieldFilter updateFieldFilter);

    public <T> T insertWith(T obj, String regex);

    public <T> T insertLinks(T obj, String regex);

    public <T> T insertRelation(T obj, String regex);

    public int update(Object obj);

    public int updateRelation(Class<?> classOfT, String regex, Chain chain, Condition cnd);

    public int updateIgnoreNull(Object obj);

    public <T> T updateWith(T obj, String regex);

    public <T> T updateLinks(T obj, String regex);

    public int delete(long id);

    public int delete(String id);

    public void delete(Long[] ids);

    public void delete(String[] ids);

    public void deleteWith(T obj, String regex);

    public void clearWith(T obj, String regex);

    public int clear();

    public int clear(String tableName);

    public int clear(Condition cnd);

    public int clear(String tableName, Condition cnd);

    public <T> T clearLinks(T obj, String regex);

    public T getField(String fieldName, long id);

    public T getField(String fieldName, String id);

    public T getField(String fieldName, Condition cnd);

    public List<T> query(String fieldName, Condition cnd);

    public List<T> query(Condition cnd);

    public List<T> query();

    public List<T> query(String linkName);

    public List<T> query(Condition cnd, String linkName);

    public List<T> query(Condition cnd, String linkName, String fieldName, Pager pager);

    public QueryResult queryPager(Condition cnd, String linkName, String fieldName, Pager pager);

    public QueryResult queryByJoinPager(Condition cnd, String linkName, String filedName1, Pager pager, Condition cnd1);

    public List<T> query(Condition cnd, String linkName, Pager pager);

    public List<T> query(Condition cnd, Pager pager);

    public List<T> query(Condition cnd, String linkName, Pager pager, FieldMatcher fieldMatcher);

    public List<T> queryByJoin(Condition cnd, String linkName, String fieldName1, Pager pager, Class<?> klass, String fieldName2);

    public List<T> queryByJoin(Condition cnd, String linkName, String filedName1, Pager pager, Condition cnd1, String filedName2);

    public List<T> queryByJoin(Condition cnd, String linkName, String fieldName1, Pager pager, Class<?> klass, String fieldName2, Class<?> klass1, String fieldName3);

    public List<T> queryByJoin(String linkName, Condition cnd);

    public <T> List<T> getByList(String sql);

    public <T> QueryResult getByListVO(Class<?> t, String sql);

    public <T> QueryResult getPagerByList(String sql, Pager pager);

    public <T> QueryResult getPagerByList(Class<?> t, String sql, Pager pager);

    public QueryResult queryPager(Pager pager, Condition cnd);

    public QueryResult queryPager(Pager pager, String linkName, Condition cnd);
}
