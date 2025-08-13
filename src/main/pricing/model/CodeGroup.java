/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accumed.pricing.model;

import com.accumed.pricing.model.request.CodeType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author smutlak
 */
public class CodeGroup {

    private CodeType type;
    private Integer id;
    private String name;
    private List<String> childGroups;
    private List<GroupCodesRange> items;

    public CodeGroup(CodeType type, Integer id, String name, String childGroups) {
        this.type = type;
        this.id = id;
        this.name = name;
        this.childGroups = new ArrayList();
        if (!childGroups.trim().isEmpty()) {
            String childGroupsArray[] = childGroups.split("\\|");
            if (childGroupsArray.length > 0) {
                this.childGroups.addAll(Arrays.asList(childGroupsArray));
            }
        }
    }

    public CodeType getType() {
        return type;
    }

    public void setType(CodeType type) {
        this.type = type;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<GroupCodesRange> getItems() {
        return items;
    }

    public void setItems(List<GroupCodesRange> items) {
        this.items = items;
    }

    public List<String> getChildGroups() {
        return childGroups;
    }

    public void setChildGroups(List<String> childGroups) {
        this.childGroups = childGroups;
    }
    
    public boolean containsCode(String sCode) {
        if(items != null && items.size()>0){
            for (GroupCodesRange item: items){
                if(item.containsCode(sCode)) {return true;}
            }
        }
        return false;
    }
    
    public boolean containsAnyCode(List<String> array) {
        for (String sCode: array){
            if(containsCode(sCode)) return true;
        }
        return false;
    }

}
