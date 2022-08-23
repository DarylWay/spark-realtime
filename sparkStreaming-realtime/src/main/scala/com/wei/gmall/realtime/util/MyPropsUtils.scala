package com.wei.gmall.realtime.util

import java.util.ResourceBundle

/**
 * 配置文件解析类
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
  def apply(propsKey: String):String = {
    bundle.getString(propsKey)
  }
}
