package org.dreams.fly.common;

import java.io.UnsupportedEncodingException;

/**
 * @author 艾国梁
 * 全局变量
 */
public final class GlobalVariable {

	/**
	 * 核心线程数
	 */
	public static final int CORE_POOL = 16;
	
	/**
	 * 最大线程数
	 */
	public static final int MAX_NUM_POOL = 50;
	
	/**
	 * 线程回收时间
	 */
	public static final long KEEP_ALIVE_TIME= 5L;
	
	
	private GlobalVariable(){

	}

	public static final class LOCK_CONSTANTS {

		public static final byte[] LOCK_DEFAULT_VALUE;

		static{
			try {
				LOCK_DEFAULT_VALUE = "1".getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		public static final int MAX_RETRY_COUNT = 3;

		public static final String REENTRANT_LOCK = "1";

		public static final String NO_REENTRANT_LOCK = "0";

		public static final String LOCK_WX_USER_SIGNIN_NAMESPACE = "WXUSER_SIGNIN";

		public static final String LOCK_PRIZE_TELEPHONERATE_NAMESPACE = "PRIZE_TELEPHONE_RATE";

		public static final String LOCK_PRIZE_VOUCHER_NAMESPACE = "PRIZE_VOUCHER";

		public static final String LOCK_PRIZE_HELPNONGKHA_NAMESPACE = "PRIZE_HELP_NONGKHA";

		public static final String LOCK_REGISTER_MEMBER_NAMESPACE = "REGISTER_MEMBER";

		public static final String LOCK_NAMESPACE ="L";

		public static final String LOCK_CAMPAIGN_PRIZE_KEY = "CAMPAIGNP_RIZE";

		public static final String LOCK_GROUP_INSERT_NAMESPACE = "GROUP_INSERT";

		public static final String LOCK_TAG_INSERT_NAMESPACE = "TAG_INSERT";

	}

	//user session key
	public static final String SESSION_KEY_USER = "user";

	/**
	 *  删除标记 正常（未删除）
	 */
	public static final String FLAG_DEL_NORMAL = "0";

	/**
	 * 删除标记 删除
	 */
	public static final String FLAG_DEL_DELETE = "1";

	/**
	 * 需要客户验证
	 */
	public static final String IS_VERIFY_CUST = "1";

	/**
	 * 不需要客户验证
	 */
	public static final String IS_NOT_VERIFY_CUST="0";

	/**
	 * 是菜单组
	 */
	public static final String IS_MENU_GROUP = "1";

	/**
	 * 不是菜单组
	 */
	public static final String IS_NOT_MENU_GROUP = "0";

	public static final String SUCCESS = "success";

	public static final String ERROR = "error";

	   /**
     * 返回的错误代码信息
     */
    public static final String RETURN_ERROR_INFO_CODE = "errcode";


    /**
     * 返回的错误信息
     */
    public static final String RETURN_ERROR_INFO_MSG = "errmsg";


    /***
     * 素材偏移值
     */
    public static final int MATERIAL_OFFSET = 20;

    //publicopenId session key
  	public static final String SESSION_KEY_PUBLICOPENID = "publicOpenId";

  	public static final int MAX_FETCH_SIZE = 10000;



  	public static final class GROUP_MESSAGE_STATUS{
  		public static final int TO_SEND = 1;
  		public static final int SENDING = 2;
  		public static final int CANCELED = 3;
  		public static final int FINISHED = 4;
  	}

  	public static final class VALIDATION_MATERIAL_STATUS{
  		public static final int UPDATE = 1;
  		public static final int ADD = 2;
  		public static final int DELETE = 3;
  	}


  	public static final class GROUP_MESSAGE_GROUP{
  		public static final int ALL = 1;
  		public static final int LABEL = 2;
  	}

  	public static final class GROUP_MESSAGE_TYPE{
  		public static final int TEXT = 1;
  		public static final int TEXT_IMG = 2;
  		public static final int IMGAE = 3;
  	}

  	public static final class GROUP_MESSAGE_SEND_MODE{
  		public static final int COMMON = 1;
  		public static final int EXPERT = 2;
  	}

  	public static final class GROUP_MESSAGE_TIMEING{
        public static final int TIMING = 1; //定时
        public static final int NONE = 2;   //非定时
    }

    public static final class GROUP_MESSAGE_SEND_TYPE{
        public static final int ACTIVE_FANS_TIMEING_EXECUTE = 11; //粉丝定时
        public static final int ACTIVE_FANS_IMMEDIATELY_EXECUTE = 12;//粉丝立即发送
        public static final int EXPERT_TIMEING_EXECUTE = 21;//群发定时
        public static final int EXPERT_IMMEDIATELY_EXECUTE = 22;//群发立即
    }

  	public static final class REPLY_MESSAGE_TYPE{
  		public static final int REPLY_WELCOME = 1;
  		public static final int DEFAULT_REPLY_MESSAGE = 2;
  	}

  	public static final class PUBLIC_ACCOUNT_TYPE{
  		public static final int DING_YUE_HAO = 1;
  		public static final int FU_WU_HAO = 2;
  		public static final int QI_YE_HAO = 3;
  	}

  	public static final class GROUP_MESSAGE_RANGE{
  		public static final int TODAY = 1;
  		public static final int LAST_WEEK = 2;
  		public static final int LAST_MONTH = 3;
  	}

  	public static final class GROUP_MESSAGE_DEL_FLAG{
  		public static final String DELETED = "1";
  		public static final String NOT_DELETEED = "0";
  	}

  	public static final class AYNC_OPT{
  		public static final String SUCCESS = "success";
  		public static final String ERROR = "error";
  	}
	
	

}
