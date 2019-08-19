package com.huawei.hwcloud.tarus.kvstore.common;

public class Ref<T> {
    T value;

    private Ref(T value) {
        this.value = value;
    }

    public static <T> Ref<T> of(Class<T> cls) {
        return new Ref(null);
    }
    
    public final T getValue() {
		return value;
	}

	public final void setValue(T value) {
		this.value = value;
	}
}