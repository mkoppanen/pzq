# - Try to find libuuid
# Once done, this will define
#
#  libuuid_FOUND - system has libuuid
#  libuuid_INCLUDE_DIRS - the libuuid include directories
#  libuuid_LIBRARIES - link these to use libuuid

include(LibFindMacros)

IF (UNIX)
	# Use pkg-config to get hints about paths
	libfind_pkg_check_modules(libuuid_PKGCONF libuuid)

	# Include dir
	find_path(libuuid_INCLUDE_DIR
	  NAMES uuid/uuid.h
	  PATHS ${LIBUUID_ROOT}/include ${libuuid_PKGCONF_INCLUDE_DIRS}
	)

	# Finally the library itself
	IF(NOT APPLE)
    	find_library(libuuid_LIBRARY
    	  NAMES uuid
    	  PATHS ${LIBUUID_ROOT}/lib ${libuuid_PKGCONF_LIBRARY_DIRS}
    	)
    	set(libuuid_PROCESS_LIBS libuuid_LIBRARY libuuid_LIBRARIES)
	ENDIF(NOT APPLE)
ENDIF()

# Set the include dir variables and the libraries and let libfind_process do the rest.
# NOTE: Singular variables for this library, plural for libraries this this lib depends on.
set(libuuid_PROCESS_INCLUDES libuuid_INCLUDE_DIR libuuid_INCLUDE_DIRS)
libfind_process(libuuid)