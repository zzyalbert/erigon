// SPDX-License-Identifier: MIT

pragma solidity >=0.6.0 <0.8.0;

contract FalsePositive  {
    string private _name;

    function name() public view returns (string memory) {
        return _name;
    }

}
