#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
]]
message("Running in src diectory")
if (${COV_ENABLED})
    add_compile_options(-fprofile-arcs -ftest-coverage)
endif ()
add_definitions(-DANTLR4CPP_STATIC)
set(ANTLR4_WITH_STATIC_CRT OFF)
add_subdirectory(parser)
add_subdirectory(common)
add_subdirectory(compress)
add_subdirectory(cwrapper)
add_subdirectory(encoding)
add_subdirectory(file)
add_subdirectory(reader)
add_subdirectory(utils)
add_subdirectory(writer)

set(COMPRESSION_LIBS snappy LZ4 lzokay zlibstatic)
target_link_libraries(parser_obj antlr4_static)
target_link_libraries(compress_obj ${COMPRESSION_LIBS})
target_link_libraries(common_obj ${COMPRESSION_LIBS})
target_link_libraries(read_obj ${COMPRESSION_LIBS})
target_link_libraries(write_obj ${COMPRESSION_LIBS})

add_library(tsfile SHARED)
if (${COV_ENABLED})
    message("Enable code cov...")
    target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj parser_obj -lgcov)
else()
    message("Disable code cov...")
    target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj parser_obj)
endif()

set(LIBTSFILE_PROJECT_VERSION ${TsFile_CPP_VERSION})
set(LIBTSFILE_SO_VERSION ${TsFile_CPP_VERSION})
set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})

set(LIBTSFILE_SDK_DIR ${LIBRARY_OUTPUT_PATH})
install(TARGETS tsfile LIBRARY DESTINATION ${LIBTSFILE_SDK_DIR})

# set(CMAKE_PREFIX_PATH ../../third-party/lz4-dev/lib)
# set(LZ4_LIB_DIR ../../third-party/lz4-dev/lib)
# find_library(my_lz4_lib NAMES lz4 PATHS ${LZ4_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
# link_directories(${LZ4_LIB_DIR})
# target_link_libraries(tsfile ${my_lz4_lib})

# if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
#   add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep liblz4 | sed 's/dylib.*/dylib/g'` ${my_lz4_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
#   add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep libz | sed 's/dylib.*/dylib/g'` ${my_z_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
# endif()
