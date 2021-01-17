pragma solidity ^0.6.11;

contract FalsePositive {
    string private _name;

    function name() public view returns (string memory) {
        return _name;
    }
}