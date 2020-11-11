package org.sn.myutils.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Rewrite of org.jetbrains.annotations.Nullable except this package is fully modularized.
 * If one requires "org.jetbrains.annotations" in module-info.java, there is a warning "requires [transitive] directive for an automatic module".
 * So use org.sn.myutils.annotations until org.jetbrains.annotations is fully modularized.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE, ElementType.TYPE_USE})
public @interface Nullable {
    String value() default "";
}