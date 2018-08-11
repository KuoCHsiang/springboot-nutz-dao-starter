package org.kuochsiang.nutz.common;

import org.nutz.dao.*;
import org.nutz.dao.entity.Entity;
import org.nutz.dao.entity.MappingField;
import org.nutz.dao.entity.PkType;
import org.nutz.dao.entity.Record;
import org.nutz.dao.pager.Pager;
import org.nutz.dao.sql.Sql;
import org.nutz.dao.util.Daos;
import org.nutz.lang.Lang;
import org.nutz.lang.Strings;
import org.nutz.service.EntityService;

import java.util.ArrayList;
import java.util.List;


public class BaseServiceImpl<T> extends EntityService<T> implements IBaseService<T> {

    public BaseServiceImpl(Dao dao) {
        super(dao);
    }

    public BaseServiceImpl() {
    }

    /**
     * 统计符合条件的对象表条数
     *
     * @param cnd 条件
     * @return
     */
    public int count(Condition cnd) {
        return this.dao().count(this.getEntityClass(), cnd);
    }

    /**
     * 统计条数
     *
     * @return
     */
    public int count() {
        return this.dao().count(this.getEntityClass());
    }

    /**
     * 统计符合条件的记录条数
     *
     * @param tableName 表名称
     * @param cnd       条件
     * @return
     */
    public int count(String tableName, Condition cnd) {
        return this.dao().count(tableName, cnd);
    }

    /**
     * 统计记录条数
     *
     * @param tableName 表名称
     * @return
     */
    public int count(String tableName) {
        return this.dao().count(tableName);
    }

    /**
     * 对某一个对象字段，进行计算
     *
     * @param funcName  函数名称 例如  sum count
     * @param fieldName 对象java对象名称
     * @return
     */
    public int func(String funcName, String fieldName) {
        return this.dao().func(this.getEntityClass(), funcName, fieldName);
    }

    /**
     * 根据条件对某一个对象字段，进行计算
     *
     * @param funcName  函数名称 例如  sum count
     * @param fieldName 对象java对象名称
     * @param cnd       条件
     * @return
     */
    public int func(String funcName, String fieldName, Condition cnd) {
        return this.dao().func(this.getEntityClass(), funcName, fieldName, cnd);
    }

    /**
     * 通过数字型主键查询对象
     *
     * @param id
     * @return
     */
    public T fetch(Long id) {
        return this.dao().fetch(this.getEntityClass(), id);
    }

    /**
     * 通过字符型主键查询对象
     *
     * @param id
     * @return
     */
    public T fetch(String id) {
        return this.dao().fetch(this.getEntityClass(), id);
    }

    /**
     * 过滤字符串查询
     *
     * @param id
     * @param fieldName
     * @return
     */
    public T fetch(Long id, String fieldName) {
        return Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName)).fetch(this.getEntityClass(), id);
    }

    /**
     * 过滤字符串查询
     *
     * @param id
     * @param fieldName
     * @return
     */
    public T fetch(String id, String fieldName) {
        return Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName)).fetch(this.getEntityClass(), id);
    }


    /**
     * 查询关联表
     *
     * @param obj   数据对象,可以是普通对象或集合,但不是类
     * @param regex 为null查询全部,支持通配符 ^(a|b)$
     * @return
     */
    public <T> T fetchLinks(T obj, String regex) {
        return this.dao().fetchLinks(obj, regex);
    }


    /**
     * 查询关联表
     *
     * @param obj   数据对象,可以是普通对象或集合,但不是类
     * @param regex 为null查询全部,支持通配符 ^(a|b)$
     * @param cnd   关联字段的过滤(排序,条件语句等)
     * @return
     */
    public <T> T fetchLinks(T obj, String regex, Condition cnd) {
        return this.dao().fetchLinks(obj, regex, cnd);
    }


    public <T> T fetchLinks(T obj, String regex, FieldFilter fieldFilter) {
        return Daos.ext(this.dao(), fieldFilter).fetchLinks(obj, regex);
    }


    /**
     * 查出符合条件的第一条记录
     *
     * @param cnd 查询条件
     * @return 实体, 如不存在则为null
     */
    public T fetch(Condition cnd) {
        return dao().fetch(getEntityClass(), cnd);
    }

    /**
     * 将一个对象插入到一个数据库
     *
     * @param obj 要被插入的对象
     *            它可以是：
     *            普通 POJO
     *            集合
     *            数组
     *            Map
     *            注意：如果是集合，数组或者 Map，所有的对象必须类型相同，否则可能会出错
     * @return 插入后的对象
     */
    public <T> T insert(T obj) {
        return this.dao().insert(obj);
    }

    /**
     * 根据对象的主键(@Id/@Name/@Pk)先查询, 如果存在就更新, 不存在就插入
     *
     * @param obj 对象
     * @return 原对象
     */
    public <T> T insertOrUpdate(T obj) {
        return this.dao().insertOrUpdate(obj);
    }


    /**
     * 根据对象的主键(@Id/@Name/@Pk)先查询, 如果存在就更新, 不存在就插入
     *
     * @param obj               对象
     * @param insertFieldFilter 插入时的字段过滤, 可以是null
     * @param updateFieldFilter 更新时的字段过滤,可以是null
     * @return 原对象
     */
    public <T> T insertOrUpdate(T obj, FieldFilter insertFieldFilter, FieldFilter updateFieldFilter) {
        return this.dao().insertOrUpdate(obj, insertFieldFilter, updateFieldFilter);
    }

    /**
     * 将对象插入数据库同时，也将符合一个正则表达式的所有关联字段关联的对象统统插入相应的数据库
     * <p>
     * 关于关联字段更多信息，请参看 '@One' | '@Many' | '@ManyMany' 更多的描述
     *
     * @param obj   数据对象
     * @param regex 正则表达式，描述了什么样的关联字段将被关注。如果为 null，则表示全部的关联字段都会被插入
     * @return 数据对象本身
     * @see org.nutz.dao.entity.annotation.One
     * @see org.nutz.dao.entity.annotation.Many
     * @see org.nutz.dao.entity.annotation.ManyMany
     */
    public <T> T insertWith(T obj, String regex) {
        return this.dao().insertWith(obj, regex);
    }

    /**
     * 根据一个正则表达式，仅将对象所有的关联字段插入到数据库中，并不包括对象本身
     *
     * @param obj   数据对象
     * @param regex 正则表达式，描述了什么样的关联字段将被关注。如果为 null，则表示全部的关联字段都会被插入
     * @return 数据对象本身
     * @see org.nutz.dao.entity.annotation.One
     * @see org.nutz.dao.entity.annotation.Many
     * @see org.nutz.dao.entity.annotation.ManyMany
     */
    public <T> T insertLinks(T obj, String regex) {
        return this.dao().insertLinks(obj, regex);
    }

    /**
     * 将对象的一个或者多个，多对多的关联信息，插入数据表
     *
     * @param obj   对象
     * @param regex 正则表达式，描述了那种多对多关联字段将被执行该操作
     * @return 对象自身
     * @see org.nutz.dao.entity.annotation.ManyMany
     */
    public <T> T insertRelation(T obj, String regex) {
        return this.dao().insertRelation(obj, regex);
    }

    /**
     * 更新数据
     *
     * @param obj
     * @return
     */
    public int update(Object obj) {
        return this.dao().update(obj);
    }


    /**
     * 多对多关联是通过一个中间表将两条数据表记录关联起来。
     * <p>
     * 而这个中间表可能还有其他的字段，比如描述关联的权重等
     * <p>
     * 这个操作可以让你一次更新某一个对象中多个多对多关联的数据
     *
     * @param classOfT 对象类型
     * @param regex    正则表达式，描述了那种多对多关联字段将被执行该操作
     * @param chain    针对中间关联表的名值链。
     * @param cnd      针对中间关联表的 WHERE 条件
     * @return 共有多少条数据被更新
     * @see org.nutz.dao.entity.annotation.ManyMany
     */
    public int updateRelation(Class<?> classOfT, String regex, Chain chain, Condition cnd) {
        return this.dao().updateRelation(classOfT, regex, chain, cnd);
    }

    /**
     * 更新数据忽略值为null的字段
     *
     * @param obj
     * @return
     */
    public int updateIgnoreNull(Object obj) {
        return this.dao().updateIgnoreNull(obj);
    }

    /**
     * 将对象更新的同时，也将符合一个正则表达式的所有关联字段关联的对象统统更新
     * <p>
     * 关于关联字段更多信息，请参看 '@One' | '@Many' | '@ManyMany' 更多的描述
     *
     * @param obj   数据对象
     * @param regex 正则表达式，描述了什么样的关联字段将被关注。如果为 null，则表示全部的关联字段都会被更新
     * @return 数据对象本身
     * @see org.nutz.dao.entity.annotation.One
     * @see org.nutz.dao.entity.annotation.Many
     * @see org.nutz.dao.entity.annotation.ManyMany
     */
    public <T> T updateWith(T obj, String regex) {
        return this.dao().updateWith(obj, regex);
    }

    /**
     * 根据一个正则表达式，仅更新对象所有的关联字段，并不包括对象本身
     *
     * @param obj   数据对象
     * @param regex 正则表达式，描述了什么样的关联字段将被关注。如果为 null，则表示全部的关联字段都会被更新
     * @return 数据对象本身
     * @see org.nutz.dao.entity.annotation.One
     * @see org.nutz.dao.entity.annotation.Many
     * @see org.nutz.dao.entity.annotation.ManyMany
     */
    public <T> T updateLinks(T obj, String regex) {
        return this.dao().updateLinks(obj, regex);
    }


    /**
     * 获取某个对象,最大的 ID 值,这个对象必须声明了 '@Id'
     *
     * @return
     */
    public int getMaxId() {
        return this.dao().getMaxId(this.getEntityClass());
    }

    /**
     * 通过long主键删除数据
     *
     * @param id
     * @return
     */
    public int delete(long id) {
        return this.dao().delete(this.getEntityClass(), id);
    }

    /**
     * 通过字符主键删除数据
     *
     * @param id
     * @return
     */
    public int delete(String id) {
        return this.dao().delete(this.getEntityClass(), id);
    }


    /**
     * 批量删除
     *
     * @param ids
     */
    public void delete(Long[] ids) {
        this.dao().clear(this.getEntityClass(), Cnd.where("id", "in", ids));
    }

    /**
     * 批量删除
     *
     * @param ids
     */
    public void delete(String[] ids) {
        this.dao().clear(this.getEntityClass(), Cnd.where("id", "in", ids));
    }

    /**
     * 关联删除         注意 此方法会把主表 副表 关联表数据全部删除
     *
     * @param obj
     * @param regex 关联字段名称
     */
    public void deleteWith(T obj, String regex) {
        this.dao().deleteWith(obj, regex);
    }

    /**
     * 优雅清除数据  删除主表数据 中间表数据 不会删除副表数据
     *
     * @param obj
     * @param regex
     */
    public void clearWith(T obj, String regex) {
        this.dao().clearLinks(obj, regex);
        this.dao().delete(obj);
    }

    /**
     * 清空表
     *
     * @return
     */
    public int clear() {
        return this.dao().clear(this.getEntityClass());
    }

    /**
     * 清空表
     *
     * @return
     */
    public int clear(String tableName) {
        return this.dao().clear(tableName);
    }

    /**
     * 按条件清除一组数据
     *
     * @return
     */
    public int clear(Condition cnd) {
        return this.dao().clear(this.getEntityClass(), cnd);
    }

    /**
     * 按条件清除一组数据
     *
     * @return
     */
    public int clear(String tableName, Condition cnd) {
        return this.dao().clear(tableName, cnd);
    }

    public <T> T clearLinks(T obj, String regex) {
        return this.dao().clearLinks(obj, regex);
    }

    /**
     * 通过LONG主键获取部分字段值
     *
     * @param fieldName
     * @param id
     * @return
     */
    public T getField(String fieldName, long id) {
        return Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName))
                .fetch(this.getEntityClass(), id);
    }

    /**
     * 通过字符主键获取部分字段值
     *
     * @param fieldName
     * @param id
     * @return
     */
    public T getField(String fieldName, String id) {
        return Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName))
                .fetch(this.getEntityClass(), id);
    }

    /**
     * 通过字符主键获取部分字段值
     *
     * @param fieldName
     * @param cnd
     * @return
     */
    public T getField(String fieldName, Condition cnd) {
        return Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName))
                .fetch(this.getEntityClass(), cnd);
    }

    /**
     * 查询获取部分字段
     *
     * @param fieldName 过滤字段 支持通配符 ^(a|b)$
     * @param cnd
     * @return
     */
    public List<T> query(String fieldName, Condition cnd) {
        return Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName))
                .query(this.getEntityClass(), cnd);
    }

    /**
     * 查询一组对象。你可以为这次查询设定条件
     *
     * @param cnd WHERE 条件。如果为 null，将获取全部数据，顺序为数据库原生顺序<br>
     *            只有在调用这个函数的时候， cnd.limit 才会生效
     * @return 对象列表
     */
    public List<T> query(Condition cnd) {
        return dao().query(getEntityClass(), cnd);
    }

    /**
     * 获取全部数据
     *
     * @return
     */
    public List<T> query() {
        return dao().query(getEntityClass(), null);
    }

    /**
     * 获取全部数据
     *
     * @param linkName 关联字段，支持正则 ^(a|b)$
     * @return
     */
    public List<T> query(String linkName) {
        return this.query(null, linkName);
    }

    /**
     * @param cnd      查询条件
     * @param linkName 关联字段，支持正则 ^(a|b)$
     * @return
     */
    public List<T> query(Condition cnd, String linkName) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd);
        if (!Strings.isBlank(linkName)) {
            this.dao().fetchLinks(list, linkName);
        }
        return list;
    }

    /**
     * 多表查询(不返回分页信息)
     *
     * @param cnd       条件
     * @param linkName  关联对象
     * @param fieldName 主表过滤字段
     * @param pager     分页
     * @return
     */
    public List<T> query(Condition cnd, String linkName, String fieldName, Pager pager) {
        List<T> list = Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName))
                .query(this.getEntityClass(), cnd, pager);
        if (!Strings.isBlank(linkName)) {
            this.dao().fetchLinks(list, linkName);
        }
        return list;
    }


    /**
     * 多表查询(回分页信息)
     *
     * @param cnd       条件
     * @param linkName  关联对象
     * @param fieldName 主表过滤字段
     * @param pager     分页
     * @return
     */
    public QueryResult queryPager(Condition cnd, String linkName, String fieldName, Pager pager) {
        List<T> list = Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName))
                .query(this.getEntityClass(), cnd, pager);
        if (!Strings.isBlank(linkName)) {
            this.dao().fetchLinks(list, linkName);
        }
        pager.setRecordCount(this.dao().count(this.getEntityClass(), cnd));
        return new QueryResult(list, pager);
    }

    /**
     * 支持主副表字段过滤的分页多表查询
     *
     * @param cnd        主表条件
     * @param linkName   关联字段
     * @param filedName1 主表过滤字段
     * @param pager      分页
     * @param cnd1       副表条件
     * @return
     */
    public QueryResult queryByJoinPager(Condition cnd, String linkName, String filedName1, Pager pager, Condition cnd1) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd, pager, filedName1);
        if (Strings.isNotBlank(linkName)) {
            Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), filedName1)).fetchLinks(list, linkName, cnd1);
        }
        pager.setRecordCount(this.dao().count(this.getEntityClass(), cnd));
        return new QueryResult(list, pager);
    }

    /**
     * 支持主副表字段过滤的分页多表查询
     *
     * @param cnd        条件   支持cnd.wrap给副表设计条件
     * @param linkName   关联字段
     * @param fieldName1 主表过滤字段
     * @param pager      分页
     * @param klass      副表对象
     * @param fieldName2 副表过滤字段
     * @return
     */
    public List<T> queryByJoin(Condition cnd, String linkName, String fieldName1, Pager pager, Class<?> klass, String fieldName2) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd, pager, fieldName1);
        if (Strings.isNotBlank(linkName)) {
            Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName1).set(klass, fieldName2)).fetchLinks(list, linkName);
        }
        return list;
    }

    /**
     * 支持主副表字段过滤的分页多表查询
     *
     * @param cnd        主表条件
     * @param linkName   关联字段
     * @param filedName1 主表过滤字段
     * @param pager      分页
     * @param cnd1       副表条件
     * @return
     */
    public List<T> queryByJoin(Condition cnd, String linkName, String filedName1, Pager pager, Condition cnd1, String filedName2) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd, pager, filedName1);
        if (Strings.isNotBlank(linkName)) {
            Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), filedName1).set(this.getEntityClass(), filedName2)).fetchLinks(list, linkName, cnd1);
        }
        return list;
    }

    /**
     * 支持主副表字段过滤的分页多表查询
     *
     * @param cnd        条件   支持cnd.wrap给副表设计条件
     * @param linkName   关联字段
     * @param fieldName1 主表过滤字段
     * @param pager      分页
     * @param klass      副表对象
     * @param fieldName2 副表过滤字段
     * @return
     */
    public List<T> queryByJoin(Condition cnd, String linkName, String fieldName1, Pager pager, Class<?> klass, String fieldName2, Class<?> klass1, String fieldName3) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd, pager, fieldName1);
        if (Strings.isNotBlank(linkName)) {
            Daos.ext(this.dao(), FieldFilter.create(this.getEntityClass(), fieldName1).set(klass, fieldName2).set(klass1, fieldName3)).fetchLinks(list, linkName);
        }
        return list;
    }

    /**
     * 分页查询
     *
     * @param pager
     * @param cnd
     * @return
     */
    public QueryResult queryPager(Pager pager, Condition cnd) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd, pager);
        pager.setRecordCount(this.dao().count(this.getEntityClass(), cnd));
        return new QueryResult(list, pager);
    }


    public List<T> query(Condition cnd, String linkName, Pager pager, FieldMatcher fieldMatcher) {
        return this.dao().query(this.getEntityClass(), cnd, pager, fieldMatcher);
    }


    /**
     * 分页关联字段查询
     *
     * @param pager    分页对象
     * @param linkName 关联字段,null为查询所有 ,支持正则 ^(a|b)$
     * @param cnd      查询条件
     * @return
     */
    public QueryResult queryPager(Pager pager, String linkName, Condition cnd) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd, pager);
        if (Strings.isNotBlank(linkName)) {
            this.dao().fetchLinks(list, linkName);
        }
        pager.setRecordCount(this.dao().count(this.getEntityClass(), cnd));
        return new QueryResult(list, pager);
    }


    /**
     * 分页关联字段查询
     *
     * @param cnd      查询条件
     * @param linkName 关联字段，支持正则 ^(a|b)$
     * @param pager    分页对象
     * @return
     */
    public List<T> query(Condition cnd, String linkName, Pager pager) {
        List<T> list = this.dao().query(this.getEntityClass(), cnd, pager);
        if (!Strings.isBlank(linkName)) {
            this.dao().fetchLinks(list, linkName);
        }
        return list;
    }

    /**
     * 分页查询
     *
     * @param cnd   查询条件
     * @param pager 分页对象
     * @return
     */
    public List<T> query(Condition cnd, Pager pager) {
        return dao().query(getEntityClass(), cnd, pager);
    }

    /**
     * 自定义sql语句获取列表
     *
     * @param sql
     * @param <T>
     * @return
     */
    public <T> List<T> getByList(String sql) {
        Sql queryRecord = Sqls.queryRecord(sql);
        this.dao().execute(queryRecord);
        List<Record> recordList = queryRecord.getList(Record.class);
        List<T> list = new ArrayList<T>();
        if (recordList != null && recordList.size() > 0) {
            Object obj = null;
            for (Record re : recordList) {
                obj = re.toEntity(this.getEntity());
                list.add((T) obj);
            }
        }
        return list;
    }

    /**
     * 自定义语句获取vo列表
     *
     * @param t
     * @param sql
     * @param <T>
     * @return
     */
    public <T> QueryResult getByListVO(Class<?> t, String sql) {
        QueryResult qr = new QueryResult();
        Sql queryRecord = Sqls.queryRecord(sql);
        this.dao().execute(queryRecord);
        List<Record> recordList = queryRecord.getList(Record.class);
        List<T> list = new ArrayList<T>();
        if (recordList != null && recordList.size() > 0) {
            Object obj = null;
            for (Record re : recordList) {
                obj = re.toEntity(this.dao().getEntity(t));
                list.add((T) obj);
            }
        }
        qr.setList(list);
        return qr;
    }

    /**
     * 一对一关联查询 可以给副表设置条件
     *
     * @param linkName 主表上 副表对象
     * @param cnd      条件
     * @return
     */
    public List<T> queryByJoin(String linkName, Condition cnd) {
        List<T> list = dao().queryByJoin(this.getEntityClass(), linkName, cnd);
        return list;
    }

    /**
     * 根据传入的条件进行分页查询
     *
     * @param sql   sql语句
     * @param pager 分页类
     * @return
     */
    public <T> QueryResult getPagerByList(String sql, Pager pager) {
        QueryResult qr = new QueryResult();
        /**先查询总行数*/
        String countStr = "select count(1) count from (" + sql + ") t";
        Sql queryRecord = Sqls.queryRecord(countStr);
        this.dao().execute(queryRecord);
        List<Record> list = queryRecord.getList(Record.class);
        if (list != null && list.size() > 0) {
            /**条数*/
            int count = list.get(0).getInt("count");
            /**查询结果*/
            Sql query = Sqls.queryRecord(sql);
            query.setPager(pager);
            this.dao().execute(query);
            list = query.getList(Record.class);
            Object obj = null;
            List<T> pagerList = new ArrayList<T>();
            for (Record re : list) {
                obj = re.toEntity(this.getEntity());
                pagerList.add((T) obj);
            }
            qr.setList(pagerList);
            /**分页构造*/
            if (null != pager) {
                pager.setRecordCount(count);
                qr.setPager(pager);
            }
        }
        return qr;
    }

    /**
     * 自定义sql专用
     *
     * @param t     对象
     * @param sql   sql语句
     * @param pager 分页类
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> QueryResult getPagerByList(Class<?> t, String sql, Pager pager) {
        QueryResult qr = new QueryResult();
        /**先查询总行数*/
        String countStr = "select count(1) count from (" + sql + ") t";
        Sql queryRecord = Sqls.queryRecord(countStr);
        this.dao().execute(queryRecord);
        List<Record> list = queryRecord.getList(Record.class);
        if (list != null && list.size() > 0) {
            /**条数*/
            int count = list.get(0).getInt("count");
            /**查询结果*/
            Sql query = Sqls.queryRecord(sql);
            query.setPager(pager);
            this.dao().execute(query);
            list = query.getList(Record.class);
            Object obj = null;
            List<T> pagerList = new ArrayList<T>();
            for (Record re : list) {
                obj = re.toEntity(this.dao().getEntity(t));
                pagerList.add((T) obj);
            }
            qr.setList(pagerList);
            /**分页构造*/
            if (null != pager) {
                pager.setRecordCount(count);
                qr.setPager(pager);
            }
        }
        return qr;
    }

    /**
     * 复杂插入/更新语句
     *
     * @param dao
     * @param obj
     * @param <T>
     * @return
     */
    public static <T> T insertOrUpdate(Dao dao, T obj) {
        if (obj == null)
            return null;
        Entity<T> en = (Entity<T>) dao.getEntity(obj.getClass());
        if (en.getPkType() == PkType.UNKNOWN)
            throw new IllegalArgumentException("no support , without pks");
        boolean doInsert = false;
        switch (en.getPkType()) {
            case ID:
                Number n = (Number) en.getIdField().getValue(obj);
                if (n == null || n.intValue() == 0)
                    doInsert = true;
                break;
            case NAME:
                if (null == en.getNameField().getValue(obj))
                    doInsert = false;
                break;
            case COMPOSITE:
                doInsert = true;
                for (MappingField mf : en.getCompositePKFields()) {
                    Object v = mf.getValue(obj);
                    if (v != null) {
                        if (v instanceof Number && ((Number) v).intValue() != 0) {
                            continue;
                        }
                        doInsert = true;
                    }
                }
            case UNKNOWN:
                throw Lang.impossible();
        }
        if (doInsert) {
            return dao.insert(obj);
        } else {
            dao.update(obj);
            return obj;
        }
    }
}
