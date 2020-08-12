package org.apache.flink.streaming.util;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ConcurrentModificationException;

/**
 * Created by jiangjg on 2017/2/24.
 */
public class MyJSONMapper {

	private static final Logger logger = LoggerFactory.getLogger(MyJSONMapper.class);
	private ObjectMapper mapper;

	public MyJSONMapper() {
		this(null);
	}

	public MyJSONMapper(JsonInclude.Include include) {
		mapper = new ObjectMapper();
		// 设置输出时包含属性的风格
		if (include != null) {
			mapper.setSerializationInclusion(include);
		}
		// 设置输入时忽略在JSON字符串中存在但Java对象实际没有的属性
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

		// 忽略EMPTY_BEANS
		mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

		// 允许单引号
		mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

		// 允许\t等控制符
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);

		// 排序
//        mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

		// DateFormat
		mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
	}

	/**
	 * 创建只输出非Null且非Empty(如List.isEmpty)的属性到Json字符串的Mapper,建议在外部接口中使用.
	 */
	public static MyJSONMapper nonEmptyMapper() {
		return new MyJSONMapper(JsonInclude.Include.NON_EMPTY);
	}

	/**
	 * 创建只输出初始值被改变的属性到Json字符串的Mapper, 最节约的存储方式，建议在内部接口中使用。
	 */
	public static MyJSONMapper nonDefaultMapper() {
		return new MyJSONMapper(JsonInclude.Include.NON_DEFAULT);
	}

	/**
	 * Object可以是POJO，也可以是Collection或数组。 如果对象为Null, 返回"null". 如果集合为空集合, 返回"[]".
	 */
	public String toJSONString(Object object) {

		try {
			return mapper.writeValueAsString(object);
		} catch (Exception ex) {
			if (ex.getCause() != null && ex.getCause() instanceof ConcurrentModificationException) {
				// toJSONString可能有ConcurrentModificationException, 最多重试9次
				int retry = 9;
				while (retry > 0) {
					try {
						logger.warn("执行toJSONString异常[ConcurrentModificationException], 重试 ...");
						return mapper.writeValueAsString(object);
					} catch (Exception cmex) {
						retry--;
					}
				}
			}
			logger.warn("执行toJSONString异常: " + object, ex);
			return null;
		}
	}

	/**
	 * Pretty
	 */
	public String toPrettyJSONString(Object object) {

		try {
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
		} catch (Exception ex) {
			if (ex.getCause() != null && ex.getCause() instanceof ConcurrentModificationException) {
				// toPrettyJSONString可能有ConcurrentModificationException, 重试9次
				int retry = 9;
				while (retry > 0) {
					try {
						logger.warn("执行toPrettyJSONString异常[ConcurrentModificationException], 重试 ...");
						return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
					} catch (Exception cmex) {
						retry--;
					}
				}
			}
			logger.warn("执行toPrettyJSONString异常: " + object, ex);
			return null;
		}
	}

	/**
	 * 反序列化POJO或简单Collection如List<String>.
	 * <p>
	 * 如果JSON字符串为Null或"null"字符串, 返回Null. 如果JSON字符串为"[]", 返回空集合.
	 * <p>
	 * 如需反序列化复杂Collection如List<MyBean>, 请使用fromJson(String,JavaType)
	 *
	 * @see #parseObject(String, JavaType)
	 */
	public <T> T parseObject(String jsonString, Class<T> clazz) {
		if (StringUtils.isEmpty(jsonString)) {
			return null;
		}

		try {
			return mapper.readValue(jsonString, clazz);
		} catch (IOException ex) {
			logger.warn("执行parseObject异常: " + jsonString, ex);
			return null;
		}
	}

	/**
	 * 反序列化复杂Collection如List<Bean>, 先使用函數createCollectionType构造类型,然后调用本函数.
	 *
	 * @see #constructCollectionType(Class, Class...)
	 */
	public <T> T parseObject(String jsonString, JavaType javaType) {
		if (StringUtils.isEmpty(jsonString)) {
			return null;
		}

		try {
			return (T) mapper.readValue(jsonString, javaType);
		} catch (IOException ex) {
			logger.warn("执行parseObject异常: " + jsonString, ex);
			return null;
		}
	}

	/**
	 * 構造泛型的Collection Type如: ArrayList<MyBean>,
	 * 则调用constructCollectionType(ArrayList.class, MyBean.class)
	 * HashMap<String, MyBean>, 则调用(HashMap.class, String.class, MyBean.class)
	 */
	public JavaType constructCollectionType(Class<?> collectionClass, Class<?>... elementClasses) {
		return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
	}

	/**
	 * 构造复杂嵌套的类型, 如: Map<String, List<MyBean>>
	 *
	 * @param collectionClass
	 * @param javaTypes
	 * @return
	 */
	public JavaType constructCollectionType(Class<?> collectionClass, JavaType... javaTypes) {
		return mapper.getTypeFactory().constructParametricType(collectionClass, javaTypes);
	}

	/**
	 * 构造简单类型
	 *
	 * @param elementClasse
	 * @return
	 */
	public JavaType constructSimpleType(Class<?> elementClasse) {
		return mapper.getTypeFactory().uncheckedSimpleType(elementClasse);
	}

	/**
	 * 构造泛型类型
	 *
	 * @return
	 */
	public JavaType constructParametrizedType(Class<?> parametrized, Class<?> parametersFor, Class<?>... parameterClasses) {
		return mapper.getTypeFactory().constructParametrizedType(parametrized, parametersFor, parameterClasses);
	}

	public ObjectMapper getMapper() {
		return mapper;
	}
}
