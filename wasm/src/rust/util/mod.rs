// Copyright 2020-2023 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::net::UdpSocket;

pub fn get_node_ip_address(address: &str) -> String {
    // convert str to String
    let mut ip_address = address.to_string();
    if ip_address.is_empty() {
        ip_address = "8.8.8.8:53".to_string();
    }
    // use udp resolver to get local ip address
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind socket");
    socket
        .connect(ip_address)
        .expect("Failed to connect to DNS server");
    let local_addr = socket.local_addr().expect("Failed to get local address");
    local_addr.ip().to_string()
}
