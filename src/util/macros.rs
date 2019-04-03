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

#[macro_export]
macro_rules! invarint {
    ($condition:expr, $($arg:tt)*) => {
        if !$condition {
            panic!($($arg)*);
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invarint_equal() {
        invarint!(true, "equal");
    }

    #[test]
    #[should_panic]
    fn test_invarint_should_panic() {
        invarint!(1 == 2, "equal");
    }
}
