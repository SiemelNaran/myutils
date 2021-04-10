package org.sn.myutils.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Rewrite of javax.annotation.concurrent.NotThreadSafe except this package is fully modularized.
 * If one requires "jsr305" in module-info.java, there is a warning "requires [transitive] directive for an automatic module".
 * So use org.sn.myutils.annotations until jsr305 is fully modularized.
 */
@Documented
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.CLASS)
public @interface NotThreadSafe {
}