// Copyright 2019 Fullstop000 <fullstop1005@gmail.com>.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

pub struct Config {
	/// Directory to store the main data in. Should exist and be writable.
    pub dir: String,
    /// Directory to store the value log in. Can be the same as Dir. Should exist and be writable.
    pub value_dir: String,
}