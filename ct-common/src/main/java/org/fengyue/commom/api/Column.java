/**
 * @Time : 2022/12/4 13:51
 * @Author : jin
 * @File : Cloumn.class
 */
package org.fengyue.commom.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

@Target({FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    String family() default "info";

    String column() default "";
}
