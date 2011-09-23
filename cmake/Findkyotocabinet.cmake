# - Try to find kyotocabinet
# Once done, this will define
#
#  kyotocabinet_FOUND - system has kyotocabinet
#  kyotocabinet_INCLUDE_DIRS - the kyotocabinet include directories
#  kyotocabinet_LIBRARIES - link these to use kyotocabinet

include(LibFindMacros)

IF (UNIX)
	# Use pkg-config to get hints about paths
	libfind_pkg_check_modules(kyotocabinet_PKGCONF kyotocabinet)

	# Include dir
	find_path(kyotocabinet_INCLUDE_DIR
	  NAMES kcdb.h
	  PATHS ${KYOTOCABINET_ROOT}/include ${kyotocabinet_PKGCONF_INCLUDE_DIRS}
	)

	# Finally the library itself
	find_library(kyotocabinet_LIBRARY
	  NAMES kyotocabinet
	  PATHS ${KYOTOCABINET_ROOT}/lib ${kyotocabinet_PKGCONF_LIBRARY_DIRS}
	)
ELSEIF (WIN32)
	find_path(kyotocabinet_INCLUDE_DIR
	  NAMES kcdb.h
	  PATHS ${KYOTOCABINET_ROOT}/include ${CMAKE_INCLUDE_PATH}
	)
	# Finally the library itself
	find_library(kyotocabinet_LIBRARY
	  NAMES kyotocabinet
	  PATHS ${KYOTOCABINET_ROOT}/lib ${CMAKE_LIB_PATH}
	)
ENDIF()

# Set the include dir variables and the libraries and let libfind_process do the rest.
# NOTE: Singular variables for this library, plural for libraries this this lib depends on.
set(kyotocabinet_PROCESS_INCLUDES kyotocabinet_INCLUDE_DIR kyotocabinet_INCLUDE_DIRS)
set(kyotocabinet_PROCESS_LIBS kyotocabinet_LIBRARY kyotocabinet_LIBRARIES)
libfind_process(kyotocabinet)