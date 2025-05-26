from __future__ import annotations

import textwrap
from importlib import metadata

APP_NAME = "flowfunc"
FLOWFUNC_LOGO = textwrap.dedent("""
   ****  **                       ****                         
  /**/  /**                      /**/                          
 ****** /**  ******  ***     ** ****** **   ** *******   ***** 
///**/  /** **////**//**  * /**///**/ /**  /**//**///** **///**
  /**   /**/**   /** /** ***/**  /**  /**  /** /**  /**/**  // 
  /**   /**/**   /** /****/****  /**  /**  /** /**  /**/**   **
  /**   ***//******  ***/ ///**  /**  //****** ***  /**//***** 
  //   ///  //////  ///    ///   //    ////// ///   //  /////  
""")

try:
    __version__ = metadata.version(APP_NAME)
except metadata.PackageNotFoundError:
    __version__ = "dev"
